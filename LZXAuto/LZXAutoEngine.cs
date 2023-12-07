using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization.Formatters.Binary;
using System.Security.Principal;
using System.Threading;
using System.Xml.Serialization;

namespace LZXAutoEngine
{
    public class LZXAutoEngine
    { public enum DbType
        {
            Nome = 0,
            Binary,
            Xml
        }

        //private const int fileSaveTimerMs = (int)30e3; //30 seconds
        private const int treadPoolWaitMs = 200;
        private const string dbFileName = "FileDict.db";

        private readonly CancellationTokenSource cancelToken = new CancellationTokenSource();
        private readonly int maxQueueLength = Environment.ProcessorCount * 16;

        private ulong compactCommandBytesRead;
        private ulong compactCommandBytesWritten;
        private uint dictEntriesCount;
        private int actualDictEntriesCount;

        private uint fileCountProcessedByCompactCommand;
        private uint fileCountSkipByNoChange;
        private uint fileCountSkippedByAttributes;
        private uint fileCountSkippedByExtension;

        private ConcurrentDictionary<string, ulong> fileDict = new ConcurrentDictionary<string, ulong>();

        private readonly object actualDbLockObject = new object();
        private ConcurrentDictionary<string, ulong> actualFileDict = new ConcurrentDictionary<string, ulong>();

#if DEBUG
        private ConcurrentDictionary<string, string> debugFileDictStr = new ConcurrentDictionary<string, string>();
#endif

        private string[] skipFileExtensions;
        private volatile int threadQueueLength;

        private ulong totalDiskBytesLogical;
        private ulong totalDiskBytesPhysical;

        public Logger logger { get; set; } = new Logger(LogLevel.General);

        public DbType dbType = DbType.Nome;
        public bool skipSystem = true;

        private readonly object threadObject = new object();
        private readonly object logObject = new object();

        private DateTime logFileStatTime;

        ~LZXAutoEngine() // finalizer
        {
            logger = null;
        }

        public bool IsElevated
        {
            get
            {
                using (var identity = WindowsIdentity.GetCurrent())
                {
                    var principal = new WindowsPrincipal(identity);
                    return principal.IsInRole(WindowsBuiltInRole.Administrator);
                }
            }
        }

        public void Process(string path, ref string[] skipFileExtensionsArr)
        {
            skipFileExtensions = skipFileExtensionsArr ?? new string[] { };

            if (path.EndsWith("\\") && path.Length > 3)
                path = path.Remove(path.Length - 1, 1);

            var driveCapacity = DriveUtils.GetDriveCapacity(path);
            var driveFreeSpace = DriveUtils.GetDriveFreeSpace(path);

            logger.Log($"{Environment.NewLine}", 1, LogLevel.Info, false);
            logger.Log(
                $"Starting new compressing session. LZXAuto version: {Assembly.GetEntryAssembly()?.GetName().Version}");
            logger.Log("Driver state:");
            logger.Log($"Capacity:{driveCapacity.GetMemoryString()}");
            logger.Log($"Free space:{driveFreeSpace.GetMemoryString()}");
            logger.Log($"Running in Administrator mode: {IsElevated}");
            logger.Log($"Starting path {path}");
            var dbTypeStr = (dbType == DbType.Nome) ? "None" : (dbType == DbType.Xml ? "Xml" : "Binary");
            logger.Log($"Database type: {dbTypeStr}{Environment.NewLine}");

            var startTimeStamp = DateTime.Now;
            logFileStatTime = startTimeStamp;

            fileDict = LoadDictFromFile(dbFileName);

            Action<DirectoryInfo> processDirectory = null;
            processDirectory = di =>
            {
                try
                {
                    var files = di.EnumerateFiles("*", SearchOption.TopDirectoryOnly);

                    foreach (var fi in files)
                        try
                        {
                            if (cancelToken.IsCancellationRequested)
                            {
                                FinalizeThreadPool();
                                break;
                            }

                            try
                            {
                                bool allowAddThread;
                                lock (threadObject)
                                {
                                    allowAddThread = threadQueueLength < maxQueueLength;

                                    if (allowAddThread)
                                    {
                                        Interlocked.Increment(ref threadQueueLength);
                                        ThreadPool.QueueUserWorkItem(a => { ProcessFile(fi); });
                                    }
                                }

                                if (allowAddThread) continue;

                                while (threadQueueLength >= maxQueueLength)
                                    Thread.Sleep(treadPoolWaitMs);
                            }
                            catch (Exception ex)
                            {
                                lock (logObject)
                                {
                                    logger.Log(ex, fi);
                                }
                            }
                        }
                        catch (UnauthorizedAccessException)
                        {
                            lock (logObject)
                            {
                                logger.Log($"Access failed to folder: {di.FullName}", 2, LogLevel.General);
                            }
                        }
                        catch (Exception ex)
                        {
                            lock (logObject)
                            {
                                logger.Log(ex, di);
                            }
                        }

                    var directories = di.EnumerateDirectories("*", SearchOption.TopDirectoryOnly);
                    foreach (var nextDi in directories) processDirectory(nextDi);
                }
                catch (Exception ex)
                {
                    lock (logObject)
                    {
                        logger.Log($"Unable to process folder {di.FullName}: {ex.Message}", 1, LogLevel.General);
                    }
                }
            };

            var dirTop = new DirectoryInfo(path);
            processDirectory(dirTop);

            // Wait until all threads complete
            FinalizeThreadPool();

            DirectoryRemoveCompressAttr(dirTop);

            logger.Flush();

            var ts = DateTime.Now.Subtract(startTimeStamp);
            logger.Log("", 1, LogLevel.Info, false);
            logger.Log(
                $"Compress completed in [hh:mm:ss:ms]: {ts.Hours:00}:{ts.Minutes:00}:{ts.Seconds:00}:{ts.Milliseconds:00}{Environment.NewLine}");
            logger.Flush();

            // actualize dbFile
            Action<DirectoryInfo> actualizeDbRecords = null;
            actualizeDbRecords = di =>
            {
                if (di.FullName == dirTop.FullName) return;

                try
                {
                    var files = di.EnumerateFiles("*", SearchOption.TopDirectoryOnly);
                    foreach (var fi in files)
                    {
                        if (cancelToken.IsCancellationRequested)
                        {
                            FinalizeThreadPool();
                            break;
                        }

                        try
                        {
                            var bytesArray = (fi.Name + fi.LastWriteTime).GenerateHashCode();
                            var nameHash = Convert.ToBase64String(bytesArray, Base64FormattingOptions.InsertLineBreaks);
                            var hashExistInOldDict = false;

                            string fullNameHash = "";
                            ulong physicalSizeClusters = 0;

                            if (fileDict.TryGetValue(nameHash, out var value))
                            {
                                physicalSizeClusters = DriveUtils.GetPhysicalFileSize(fi.FullName);
                                hashExistInOldDict = (value == physicalSizeClusters);
                            }

                            if (!hashExistInOldDict)
                            {
                                fullNameHash = Convert.ToBase64String(fi.FullName.GenerateHashCode(),
                                    Base64FormattingOptions.InsertLineBreaks);
                                if (physicalSizeClusters == 0)
                                    physicalSizeClusters = DriveUtils.GetPhysicalFileSize(fi.FullName);
                                hashExistInOldDict = fileDict.ContainsKey(fullNameHash) &&
                                                   fileDict[fullNameHash] == physicalSizeClusters;
                            }

                            if (hashExistInOldDict)
                            {
                                lock (actualDbLockObject)
                                {
                                    var result = AddRecordToActualFileDict(ref nameHash, ref fullNameHash, fi, physicalSizeClusters);
                                    if (result) ++actualDictEntriesCount;

                                    DebugCheckDuplicates(ref nameHash, ref fullNameHash, fi);

                                    Console.WriteLine($"Database records: {actualDictEntriesCount}");
                                    Console.SetCursorPosition(0, Console.CursorTop - 1);
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            logger.Log(ex, fi);
                        }
                    }

                    var directories = di.EnumerateDirectories("*", SearchOption.TopDirectoryOnly);
                    foreach (var nextDi in directories) actualizeDbRecords(nextDi);
                }
                catch (Exception)
                {
                    // ignored
                }
            };

            if (dbType != DbType.Nome)
            {
                var actualizeDbTimeStamp = DateTime.Now;

                var driveRoot = new DirectoryInfo(path.Substring(0, 2) + "\\");
                actualizeDbRecords(driveRoot);

                SaveDictToFile(dbFileName, ref actualFileDict);

                ts = DateTime.Now.Subtract(actualizeDbTimeStamp);
                logger.Log(
                    $"Actualize database completed in [hh:mm:ss:ms]: {ts.Hours:00}:{ts.Minutes:00}:{ts.Seconds:00}:{ts.Milliseconds:00}{Environment.NewLine}",
                    2);
                logger.Flush();
            }

            logger.Log("All operations completed.");

            ts = DateTime.Now.Subtract(startTimeStamp);
            var totalFilesVisited = fileCountProcessedByCompactCommand + fileCountSkipByNoChange +
                                    fileCountSkippedByAttributes + fileCountSkippedByExtension;

            var spaceSavings = "-";
            if (compactCommandBytesRead > 0)
                spaceSavings = $"{(1 - (decimal)compactCommandBytesWritten / compactCommandBytesRead) * 100m:0.00}%";

            var compressionRatio = "-";
            if (compactCommandBytesWritten > 0)
                compressionRatio = $"{(decimal)compactCommandBytesRead / compactCommandBytesWritten:0.00}";

            var spaceSaving = "-";
            if (totalDiskBytesLogical > 0)
                spaceSaving = $"{(1 - (decimal)totalDiskBytesPhysical / totalDiskBytesLogical) * 100m:0.00}%";

            var totalPhysicalRatio = "-";
            if (totalDiskBytesLogical > 0)
                totalPhysicalRatio = $"{(decimal)totalDiskBytesLogical / totalDiskBytesPhysical:0.00}";

            logger.Log(
                $"Stats for this session: {Environment.NewLine}" +
                $"Files skipped by attributes: {fileCountSkippedByAttributes}{Environment.NewLine}" +
                $"Files skipped by extension: {fileCountSkippedByExtension}{Environment.NewLine}" +
                $"Files skipped by no change: {fileCountSkipByNoChange}{Environment.NewLine}" +
                $"Files processed by compact command line: {fileCountProcessedByCompactCommand}{Environment.NewLine}"
                , 2, LogLevel.General);

            if (dbType != DbType.Nome)
            {
                logger.Log(
                    $"Files in db: {actualDictEntriesCount}{Environment.NewLine}" +
                    $"Files in db delta: {actualDictEntriesCount - dictEntriesCount}{Environment.NewLine}"
                    , 1, LogLevel.General);
            }

            logger.Log($"Files visited: {totalFilesVisited}{Environment.NewLine}" +
                   $"{Environment.NewLine}" +
                   $"Bytes read: {compactCommandBytesRead.GetMemoryString()}{Environment.NewLine}" +
                   $"Bytes written: {compactCommandBytesWritten.GetMemoryString()}{Environment.NewLine}" +
                   $"Space savings bytes: {(compactCommandBytesRead - compactCommandBytesWritten).GetMemoryString()}{Environment.NewLine}" +
                   $"Space savings: {spaceSavings}{Environment.NewLine}" +
                   $"Compression ratio: {compressionRatio}{Environment.NewLine}{Environment.NewLine}" +
                   $"Disk stat:{Environment.NewLine}" +
                   $"Files logical size: {totalDiskBytesLogical.GetMemoryString()}{Environment.NewLine}" +
                   $"Files physical size: {totalDiskBytesPhysical.GetMemoryString()}{Environment.NewLine}" +
                   $"Space savings: {spaceSaving}{Environment.NewLine}" +
                   $"Compression ratio: {totalPhysicalRatio}"
                , 1, LogLevel.General);

            logger.Log(
                $"Perf stats:{Environment.NewLine}" +
                $"Time elapsed[hh:mm:ss:ms]: {ts.Hours:00}:{ts.Minutes:00}:{ts.Seconds:00}:{ts.Milliseconds:00}{Environment.NewLine}" +
                $"Compressed files per minute: {fileCountProcessedByCompactCommand / ts.TotalMinutes:0.00}{Environment.NewLine}" +
                $"Files per minute: {totalFilesVisited / ts.TotalMinutes:0.00}", 2, LogLevel.General, false);

            logger.Flush();
        }

        private void ProcessFile(FileInfo fi)
        {
            try
            {
                if (fi.Length <= 0) return;

                var prevPhysicalSizeClusters = DriveUtils.GetPhysicalFileSize(fi.FullName);
                if (prevPhysicalSizeClusters == 0) return;

                var logicalSizeClusters = DriveUtils.GetDiskOccupiedSpace((ulong)fi.Length, fi.FullName);

                if (skipFileExtensions.Any(c => c == fi.Extension))
                {
                    lock (logObject)
                    {
                        ThreadUtils.InterlockedIncrement(ref fileCountSkippedByExtension);
                        logger.Log($"Skip file '{fi.FullName}' by extensions list.", 1, LogLevel.Debug);

                        UpdateAndShowDiskStats(ref logicalSizeClusters, ref prevPhysicalSizeClusters);
                    }

                    return;
                }

                if (skipSystem && fi.Attributes.HasFlag(FileAttributes.System))
                {
                    lock (logObject)
                    {
                        ThreadUtils.InterlockedIncrement(ref fileCountSkippedByAttributes);
                        logger.Log($"Skip file '{fi.FullName}' by system flag.", 1, LogLevel.Debug);

                        UpdateAndShowDiskStats(ref logicalSizeClusters, ref prevPhysicalSizeClusters);
                    }

                    return;
                }

                //logger.Log("", 2, LogLevel.Debug);
                var bytesArray = (fi.Name + fi.LastWriteTime).GenerateHashCode();
                var nameHash = Convert.ToBase64String(bytesArray, Base64FormattingOptions.InsertLineBreaks);
                var fullNameHash = "";

                if (prevPhysicalSizeClusters != logicalSizeClusters && !fi.Attributes.HasFlag(FileAttributes.Compressed))
                {
                    lock (actualDbLockObject)
                    {
                        var result = AddRecordToActualFileDict(ref nameHash, ref fullNameHash, fi, prevPhysicalSizeClusters);
                        if (result) ++actualDictEntriesCount;

                        DebugCheckDuplicates(ref nameHash, ref fullNameHash, fi);

                        lock (logObject)
                        {
                            ThreadUtils.InterlockedIncrement(ref fileCountSkipByNoChange);
                            logger.Log(
                                $"Skip file '{fi.FullName}' because it has been visited already and its size ('{fi.Length.GetMemoryString()}') did not change",
                                1, LogLevel.Debug);

                            UpdateAndShowDiskStats(ref logicalSizeClusters, ref prevPhysicalSizeClusters);
                        }
                    }

                    return;
                }

                var useForceCompress = false;
                if (fi.Attributes.HasFlag(FileAttributes.Compressed))
                {
                    File.SetAttributes(fi.FullName, fi.Attributes & ~FileAttributes.Compressed);
                    useForceCompress = true;
                }

                var alreadyCompressed = fileDict.ContainsKey(nameHash) && fileDict[nameHash] == prevPhysicalSizeClusters;
                if (!alreadyCompressed)
                {
                    fullNameHash = Convert.ToBase64String(fi.FullName.GenerateHashCode(), Base64FormattingOptions.InsertLineBreaks);
                    alreadyCompressed = fileDict.ContainsKey(fullNameHash) && fileDict[fullNameHash] == prevPhysicalSizeClusters;
                }

                if (alreadyCompressed)
                {
                    lock (actualDbLockObject)
                    {
                        var result = AddRecordToActualFileDict(ref nameHash, ref fullNameHash, fi, prevPhysicalSizeClusters);
                        if (result) ++actualDictEntriesCount;

                        DebugCheckDuplicates(ref nameHash, ref fullNameHash, fi);

                        lock (logObject)
                        {
                            ThreadUtils.InterlockedIncrement(ref fileCountSkipByNoChange);
                            logger.Log(
                                $"Skip file '{fi.FullName}' because it has been visited already and its size ('{fi.Length.GetMemoryString()}') did not change",
                                1, LogLevel.Debug);

                            UpdateAndShowDiskStats(ref logicalSizeClusters, ref prevPhysicalSizeClusters);
                        }
                    }

                    return;
                }

                lock (logObject)
                {
                    logger.Log($"Compressing file {fi.FullName}", 1, LogLevel.Debug);
                }

                ThreadUtils.InterlockedIncrement(ref fileCountProcessedByCompactCommand);

                var outPut = CompactCommand($"/c /exe:LZX {(useForceCompress ? "/f" : "")} \"{fi.FullName}\"");

                var currentPhysicalSizeClusters = DriveUtils.GetPhysicalFileSize(fi.FullName);
                lock (actualDbLockObject)
                {
                    var result = AddRecordToActualFileDict(ref nameHash, ref fullNameHash, fi, currentPhysicalSizeClusters);
                    if (result) ++actualDictEntriesCount;

                    DebugCheckDuplicates(ref nameHash, ref fullNameHash, fi);

                    lock (logObject)
                    {
                        ThreadUtils.InterlockedAdd(ref compactCommandBytesRead, ref prevPhysicalSizeClusters);
                        ThreadUtils.InterlockedAdd(ref compactCommandBytesWritten, ref currentPhysicalSizeClusters);
                        logger.Log($"File stats: {prevPhysicalSizeClusters} => {currentPhysicalSizeClusters}, name: {fi.FullName}");

                        UpdateAndShowDiskStats(ref logicalSizeClusters, ref prevPhysicalSizeClusters);
                    }
                }
            }
            catch (UnauthorizedAccessException)
            {
                lock (logObject)
                {
                    logger.Log($"Error during processing file: {fi.FullName}.{Environment.NewLine}Exception details: System.UnauthorizedAccessException");
                }
            }
            catch (PathTooLongException)
            {
                lock (logObject)
                {
                    logger.Log($"Error during processing file: {fi.FullName}.{Environment.NewLine}Exception details: System.IO.PathTooLongException");
                }
            }
            catch (Exception ex)
            {
                lock (logObject)
                {
                    logger.Log(ex, fi);
                }
            }
            finally
            {
                Interlocked.Decrement(ref threadQueueLength);
            }
        }

        private void UpdateAndShowDiskStats(ref ulong logicalSize, ref ulong physicalSize)
        {
            ThreadUtils.InterlockedAdd(ref totalDiskBytesLogical, ref logicalSize);
            ThreadUtils.InterlockedAdd(ref totalDiskBytesPhysical, ref physicalSize);

            var tempTime = DateTime.Now;
            if ((tempTime - logFileStatTime).TotalSeconds == 0) return;

            logFileStatTime = tempTime;
            logger.ShowDiskStats(actualDictEntriesCount, ref totalDiskBytesPhysical, ref totalDiskBytesLogical);
        }

        private bool AddRecordToActualFileDict(ref string nameHash, ref string fullNameHash, FileInfo fi, ulong fileSize)
        {
            if (actualFileDict.ContainsKey(nameHash))
            {
                if (fullNameHash.Length == 0)
                    fullNameHash = Convert.ToBase64String(fi.FullName.GenerateHashCode(), Base64FormattingOptions.InsertLineBreaks);
                return actualFileDict.TryAdd(fullNameHash, fileSize);
            }

            return actualFileDict.TryAdd(nameHash, fileSize);
        }

        private void DebugCheckDuplicates(ref string nameHash, ref string fullNameHash, FileInfo fi)
        {
#if DEBUG
            var hash = fullNameHash.Length == 0 ? nameHash : fullNameHash;

            if (debugFileDictStr.TryGetValue(hash, out var value))
            {
                lock (logObject)
                {
                    logger.Log($"Replace record in debug with name: {value} => {fi.FullName}");
                }
            }
            else
            {
                debugFileDictStr.TryAdd(hash, fi.FullName);
            }
#endif
        }

        private void DirectoryRemoveCompressAttr(DirectoryInfo dirTop)
        {
            if (!dirTop.Attributes.HasFlag(FileAttributes.Compressed)) return;

            logger.Log($"Removing NTFS compress flag on folder {dirTop.FullName} in favor of LZX compression", 1,
                LogLevel.General);

            var outPut = CompactCommand($"/u \"{dirTop.FullName}\"");

            logger.Log(outPut, 2, LogLevel.Debug);
        }

        private string CompactCommand(string arguments)
        {
            var proc = new Process();
            proc.StartInfo.FileName = "compact";
            proc.StartInfo.Arguments = arguments;
            proc.StartInfo.UseShellExecute = false;
            proc.StartInfo.RedirectStandardOutput = true;

            logger.Log(arguments, 1, LogLevel.Debug);

            proc.Start();
            try
            {
                proc.PriorityClass = ProcessPriorityClass.Idle;
            }
            catch (InvalidOperationException)
            {
                logger.Log("Process Compact exited before setting its priority. Nothing to worry about.", 3,
                    LogLevel.Debug);
            }

            var outPut = proc.StandardOutput.ReadToEnd();
            proc.WaitForExit();
            proc.Close();

            return outPut;
        }

        public void ResetDb()
        {
            File.Delete(dbFileName);
        }

        public void SaveDictToFile(string fileName, ref ConcurrentDictionary<string, ulong> concDict)
        {
            if (dbType == DbType.Nome) return;

            try
            {
                lock (ThreadUtils.lockObject)
                {
                    if (concDict.Count == 0) return;

                    var items = new SerializeDataItemList(concDict.Count);

                    foreach (var hash in concDict)
                        items.Add(new SerializeDataItem(hash.Key, hash.Value));

                    using (var writerFileStream = new FileStream(fileName, FileMode.Create, FileAccess.Write))
                    {
                        logger.Log("Saving file...", 1, LogLevel.Debug);

                        if (dbType == DbType.Binary)
                        {
                            var binaryFormatter = new BinaryFormatter();

                            var dict = concDict.ToDictionary(a => a.Key, b => b.Value);
                            binaryFormatter.Serialize(writerFileStream, dict);
                        }
                        else if (dbType == DbType.Xml)
                        {
                            var serializer = new XmlSerializer(typeof(SerializeDataItemList));
                            var ns = new XmlSerializerNamespaces();
                            ns.Add("", "");
                            serializer.Serialize(writerFileStream, items, ns);
                        }

                        logger.Log($"File saved, dictCount: {concDict.Count}, fileSize: {writerFileStream.Length}", 1,
                            LogLevel.Debug);

                        writerFileStream.Close();
                    }
                }
            }
            catch (Exception ex)
            {
                logger.Log($"Unable to save dic to file, {ex.Message}");
            }
        }

        public ConcurrentDictionary<string, ulong> LoadDictFromFile(string fileName)
        {
            var retVal = new ConcurrentDictionary<string, ulong>();

            if (dbType == DbType.Nome) return retVal;

            if (File.Exists(fileName))
                try
                {
                    logger.Log("Dictionary file found");

                    retVal.Clear();
                    dictEntriesCount = 0;

                    using (var readerFileStream = new FileStream(fileName, FileMode.Open, FileAccess.Read))
                    {
                        if (readerFileStream.Length > 0)
                        {
                            var deserializeData = new object();
                            if (dbType == DbType.Binary)
                            {
                                var binaryFormatter = new BinaryFormatter();

                                try
                                {
                                    deserializeData = binaryFormatter.Deserialize(readerFileStream);
                                }
                                catch (Exception)
                                {
                                    // ignored
                                }
                            }
                            else if (dbType == DbType.Xml)
                            {
                                var serial = new XmlSerializer(typeof(SerializeDataItemList),
                                    new[] { typeof(SerializeDataItemList) });

                                try
                                {
                                    deserializeData = serial.Deserialize(readerFileStream);
                                }
                                catch (Exception)
                                {
                                    // ignored
                                }
                            }

                            if (deserializeData is SerializeDataItemList tempList)
                            {
                                foreach (var item in tempList.ToList())
                                    retVal.TryAdd(item.Key, item.Value);

                                dictEntriesCount = (uint)retVal.Count;
                            }

                            readerFileStream.Close();
                        }
                    }

                    logger.Log($"Loaded from file ({dictEntriesCount} entries){Environment.NewLine}");
                }
                catch (Exception ex)
                {
                    logger.Log($"Error during loading from file: {ex.Message}" +
                               $"{Environment.NewLine}Terminating.");

                    Environment.Exit(-1);
                }
            else
                logger.Log("DB file not found");

            return retVal;
        }

        //private void FileSaveTimerCallback(object state)
        //{
        //    logger.Log("Saving dictionary file...", 1, LogFlags.Debug);
        //    SaveDictToFile();
        //}

        public void Cancel()
        {
            logger.Log("Terminating...", 3, LogLevel.General);
            cancelToken.Cancel();
        }

        private void FinalizeThreadPool()
        {
            // Disable file save timer callback
            //timer.Change(Timeout.Infinite, Timeout.Infinite);

            // Wait for thread pool to complete
            while (threadQueueLength > 0) Thread.Sleep(treadPoolWaitMs);
        }

        [XmlRoot("Object")]
        public class SerializeDataItem
        {
            public string Key;
            public ulong Value;

            public SerializeDataItem()
            {
                Value = 0;
            }

            public SerializeDataItem(string key, ulong value)
            {
                Key = key;
                Value = value;
            }
        }

        [XmlRoot("HashList")]
        public class SerializeDataItemList
        {
            [XmlArrayItem("HashPair", typeof(SerializeDataItem))]
            public List<SerializeDataItem> Objects;

            // Constructor
            public SerializeDataItemList()
            {
                Objects = new List<SerializeDataItem>();
            }

            public SerializeDataItemList(int count)
            {
                Objects = new List<SerializeDataItem>(count);
            }

            public void Add(SerializeDataItem serializeDataItem)
            {
                Objects.Add(serializeDataItem);
            }

            public List<SerializeDataItem> ToList()
            {
                return Objects;
            }
        }
    }
}