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
    {
        //private const int fileSaveTimerMs = (int)30e3; //30 seconds
        private const int treadPoolWaitMs = 200;
        private const string dbFileName = "FileDict.db";

        private readonly CancellationTokenSource cancelToken = new CancellationTokenSource();
        private readonly int maxQueueLength = Environment.ProcessorCount * 16;

        private ulong compactCommandBytesRead;
        private ulong compactCommandBytesWritten;
        private uint dictEntriesCount0;

        private uint fileCountProcessedByCompactCommand;
        private uint fileCountSkipByNoChange;
        private uint fileCountSkippedByAttributes;
        private uint fileCountSkippedByExtension;
        private ConcurrentDictionary<byte[], ulong> fileDict = new ConcurrentDictionary<byte[], ulong>();
        private ConcurrentDictionary<byte[], ulong> actualFileDict = new ConcurrentDictionary<byte[], ulong>();

#if DEBUG
        private ConcurrentDictionary<byte[], string> debugFileDictStr = new ConcurrentDictionary<byte[], string>();
#endif

        private string[] skipFileExtensions;
        private int threadQueueLength;

        private ulong totalDiskBytesLogical;
        private ulong totalDiskBytesPhysical;

        public Logger logger { get; set; } = new Logger(LogLevel.General);

        public bool binaryDb = false;
        public bool skipSystem = true;

        private readonly object actualDbLockObject = new object();

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

            var driveCapacity = DriveUtils.GetDriveCapacity(path);
            var driveFreeSpace = DriveUtils.GetDriveFreeSpace(path);

            logger.Log($"{Environment.NewLine}", 1, LogLevel.Info, false);
            logger.Log(
                $"Starting new compressing session. LZXAuto version: {Assembly.GetEntryAssembly()?.GetName().Version}");
            logger.Log($"Driver state:");
            logger.Log($"Capacity:{driveCapacity.GetMemoryString()}");
            logger.Log($"Free space:{driveFreeSpace.GetMemoryString()}");
            logger.Log($"Running in Administrator mode: {IsElevated}");
            logger.Log($"Starting path {path}{Environment.NewLine}");

            var startTimeStamp = DateTime.Now;

            fileDict = LoadDictFromFile(dbFileName);

            Action<DirectoryInfo> processDirectory = null;
            processDirectory = (di) =>
            {
                try
                {
                    var files = di.EnumerateFiles("*", SearchOption.TopDirectoryOnly);

                    foreach (var fi in files)
                    {
                        try
                        {
                            if (cancelToken.IsCancellationRequested)
                            {
                                FinalizeThreadPool();
                                break;
                            }

                            try
                            {
                                Interlocked.Increment(ref threadQueueLength);
                                ThreadPool.QueueUserWorkItem(a => { ProcessFile(fi); });

                                // Do not let queue length more items than MaxQueueLength
                                while (threadQueueLength > maxQueueLength)
                                    Thread.Sleep(treadPoolWaitMs);
                            }
                            catch (Exception ex)
                            {
                                logger.Log(ex, fi);
                            }
                        }
                        catch (UnauthorizedAccessException)
                        {
                            logger.Log($"Access failed to folder: {di.FullName}", 2, LogLevel.General);
                        }
                        catch (Exception ex)
                        {
                            logger.Log(ex, di);
                        }
                    }

                    var directories = di.EnumerateDirectories("*", SearchOption.TopDirectoryOnly);
                    foreach (var nextDi in directories)
                    {
                        processDirectory(nextDi);
                    }
                }
                catch (Exception ex)
                {
                    logger.Log($"Unable to process folder {di.FullName}: {ex.Message}", 1, LogLevel.General);
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
            actualizeDbRecords = (di) =>
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
                            var filePathHash = (fi.Name + fi.LastWriteTime).GenerateHashCode();

                            lock (actualDbLockObject)
                            {
                                if (fileDict.TryGetValue(filePathHash, out var dictFileSize))
                                {
                                    var physicalSizeClusters = DriveUtils.GetPhysicalFileSize(fi.FullName);

                                    if (dictFileSize == physicalSizeClusters)
                                    {
                                        fileDict.TryRemove(filePathHash, out _);
                                        actualFileDict[filePathHash] = physicalSizeClusters;
                                    }
                                }
                            }

                            // Do not let queue length more items than MaxQueueLength
                            while (threadQueueLength > maxQueueLength)
                                Thread.Sleep(treadPoolWaitMs);
                        }
                        catch (Exception ex)
                        {
                            logger.Log(ex, fi);
                        }
                    }

                    var directories = di.EnumerateDirectories("*", SearchOption.TopDirectoryOnly);
                    foreach (var nextDi in directories)
                    {
                        actualizeDbRecords(nextDi);
                    }
                }
                catch (Exception)
                {
                    // ignored
                }
            };

            var actualizeDbTimeStamp = DateTime.Now;

            var driveRoot = new DirectoryInfo(path.Substring(0, 2) + "\\");
            actualizeDbRecords(driveRoot);

            SaveDictToFile(dbFileName, actualFileDict);

            ts = DateTime.Now.Subtract(actualizeDbTimeStamp);
            logger.Log(
                $"Actualize database completed in [hh:mm:ss:ms]: {ts.Hours:00}:{ts.Minutes:00}:{ts.Seconds:00}:{ts.Milliseconds:00}{Environment.NewLine}");
            logger.Flush();

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
                $"Files processed by compact command line: {fileCountProcessedByCompactCommand}{Environment.NewLine}" +
                $"Files in db: {actualFileDict?.Count ?? 0}{Environment.NewLine}" +
                $"Files in db delta: {(actualFileDict?.Count ?? 0) - dictEntriesCount0}{Environment.NewLine}" +
                $"Files visited: {totalFilesVisited}{Environment.NewLine}" +
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
                $"Files per minute: {totalFilesVisited / ts.TotalMinutes:0.00}", 1, LogLevel.General, false);

            logger.Flush();
        }

        private void ProcessFile(FileInfo fi)
        {
            try
            {
                var prevPhysicalSizeClusters = DriveUtils.GetPhysicalFileSize(fi.FullName);
                if (prevPhysicalSizeClusters == 0) return;

                var logicalSizeClusters = DriveUtils.GetDiskOccupiedSpace((ulong)fi.Length, fi.FullName);
                ThreadUtils.InterlockedAdd(ref totalDiskBytesLogical, logicalSizeClusters);

                if (skipFileExtensions.Any(c => c == fi.Extension))
                {
                    logger.Log($"Skipping file: '{fi.FullName}' by extensions list.", 1, LogLevel.Debug);
                    ThreadUtils.InterlockedIncrement(ref fileCountSkippedByExtension);
                    ThreadUtils.InterlockedAdd(ref totalDiskBytesPhysical, prevPhysicalSizeClusters);
                    return;
                }

                if (skipSystem && fi.Attributes.HasFlag(FileAttributes.System))
                {
                    logger.Log($"Skipping file: '{fi.FullName}' by system flag.", 1, LogLevel.Debug);
                    ThreadUtils.InterlockedIncrement(ref fileCountSkippedByAttributes);
                    ThreadUtils.InterlockedAdd(ref totalDiskBytesPhysical, prevPhysicalSizeClusters);
                    return;
                }

                var useForceCompress = false;
                if (fi.Attributes.HasFlag(FileAttributes.Compressed))
                {
                    File.SetAttributes(fi.FullName, fi.Attributes & ~FileAttributes.Compressed);
                    useForceCompress = true;
                }

                if (fi.Length > 0)
                {
                    //logger.Log("", 2, LogLevel.Debug);

                    var fileHash = (fi.Name + fi.LastWriteTime).GenerateHashCode();

                    lock (actualDbLockObject)
                    {
                        bool alreadyCompressed = fileDict.TryGetValue(fileHash, out var dictFileSize1) &&
                                                 (dictFileSize1 == prevPhysicalSizeClusters);
                        if (alreadyCompressed)
                        {
                            logger.Log(
                                $"Skipping file: '{fi.FullName}' because it has been visited already and its size ('{fi.Length.GetMemoryString()}') did not change",
                                1, LogLevel.Debug);
                            ThreadUtils.InterlockedIncrement(ref fileCountSkipByNoChange);
                            ThreadUtils.InterlockedAdd(ref totalDiskBytesPhysical, prevPhysicalSizeClusters);
                            actualFileDict[fileHash] = prevPhysicalSizeClusters;
#if DEBUG
                            var debugHash = fi.FullName.GenerateHashCode();
                            bool debugHasValue = debugFileDictStr.TryGetValue(debugHash, out var dictName3);
                            if (debugHasValue)
                            {
                                logger.Log($"Replace record in debug with name: {dictName3} => {fi.FullName}");
                            }
                            else
                            {
                                debugFileDictStr[debugHash] = fi.FullName;
                            }
#endif
                            return;
                        }
                    }

                    logger.Log($"Compressing file {fi.FullName}", 1, LogLevel.Debug);
                    ThreadUtils.InterlockedIncrement(ref fileCountProcessedByCompactCommand);

                    var outPut = CompactCommand($"/c /exe:LZX {(useForceCompress ? "/f" : "")} \"{fi.FullName}\"");

                    var currentPhysicalSizeClusters = DriveUtils.GetPhysicalFileSize(fi.FullName);
                    lock (actualDbLockObject)
                    {
                        actualFileDict[fileHash] = currentPhysicalSizeClusters;
#if DEBUG
                        var debugHash3 = fi.FullName.GenerateHashCode();
                        bool debugHasValue3 = debugFileDictStr.TryGetValue(debugHash3, out var dictName3);
                        if (debugHasValue3)
                        {
                            logger.Log($"Replace record in debug with name: {dictName3} => {fi.FullName}");
                        }
                        else
                        {
                            debugFileDictStr[debugHash3] = fi.FullName;
                        }
#endif
                    }

                    if (prevPhysicalSizeClusters > currentPhysicalSizeClusters)
                    {
                        logger.Log(
                            $"DiskSize: {prevPhysicalSizeClusters} => {currentPhysicalSizeClusters}, fileName: {fi.FullName}");
                    }

                    ThreadUtils.InterlockedAdd(ref compactCommandBytesRead, prevPhysicalSizeClusters);
                    ThreadUtils.InterlockedAdd(ref compactCommandBytesWritten, currentPhysicalSizeClusters);
                    ThreadUtils.InterlockedAdd(ref totalDiskBytesPhysical, currentPhysicalSizeClusters);

                    logger.Log(outPut, 2, LogLevel.Debug);
                }
            }
            catch (UnauthorizedAccessException)
            {
                logger.Log(
                    $"Error during processing file: {fi.FullName}.{Environment.NewLine}Exception details: System.UnauthorizedAccessException");
            }
            catch (PathTooLongException)
            {
                logger.Log(
                    $"Error during processing file: {fi.FullName}.{Environment.NewLine}Exception details: System.IO.PathTooLongException");
            }
            catch (Exception ex)
            {
                logger.Log(ex, fi);
            }
            finally
            {
                Interlocked.Decrement(ref threadQueueLength);
            }
        }

        private void DirectoryRemoveCompressAttr(DirectoryInfo dirTop)
        {
            if (dirTop.Attributes.HasFlag(FileAttributes.Compressed))
            {
                logger.Log($"Removing NTFS compress flag on folder {dirTop.FullName} in favor of LZX compression", 1,
                    LogLevel.General);

                var outPut = CompactCommand($"/u \"{dirTop.FullName}\"");

                logger.Log(outPut, 2, LogLevel.Debug);
            }
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

        public void SaveDictToFile(string fileName, ConcurrentDictionary<byte[], ulong> concDict)
        {
            try
            {
                lock (ThreadUtils.lockObject)
                {
                    var items = new SerializeDataItemList(concDict.Count);
                    foreach (var key in concDict.Keys)
                        items.Add(new SerializeDataItem(key, concDict[key]));

                    using (FileStream writerFileStream = new FileStream(fileName, FileMode.Create, FileAccess.Write))
                    {
                        logger.Log("Saving file...", 1, LogLevel.Debug);

                        if (binaryDb)
                        {
                            BinaryFormatter binaryFormatter = new BinaryFormatter();

                            var dict = concDict.ToDictionary(a => a.Key, b => b.Value);
                            binaryFormatter.Serialize(writerFileStream, dict);
                        }
                        else
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

        public ConcurrentDictionary<byte[], ulong> LoadDictFromFile(string fileName)
        {
            var retVal = new ConcurrentDictionary<byte[], ulong>();

            if (File.Exists(fileName))
                try
                {
                    logger.Log("Dictionary file found");

                    using (var readerFileStream = new FileStream(fileName, FileMode.Open, FileAccess.Read))
                    {
                        if (readerFileStream.Length > 0)
                        {
                            if (binaryDb)
                            {
                                BinaryFormatter binaryFormatter = new BinaryFormatter();

                                try
                                {
                                    var dict = binaryFormatter.Deserialize(readerFileStream);
                                    retVal = new ConcurrentDictionary<byte[], ulong>((Dictionary<byte[], ulong>)dict);
                                }
                                catch (Exception)
                                {
                                    // ignored
                                }
                            }
                            else
                            {
                                var serial = new XmlSerializer(typeof(SerializeDataItemList),
                                    new[] { typeof(SerializeDataItemList) });

                                retVal.Clear();
                                try
                                {
                                    if (serial.Deserialize(readerFileStream) is List<SerializeDataItem> tempList)
                                        foreach (var item in tempList)
                                            retVal.TryAdd(item.Key, item.Value);
                                }
                                catch (Exception)
                                {
                                    // ignored
                                }
                            }

                            readerFileStream.Close();
                        }
                    }

                    dictEntriesCount0 = (uint)retVal.Count;

                    logger.Log($"Loaded from file ({dictEntriesCount0} entries){Environment.NewLine}");
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
            public byte[] Key;
            public ulong Value;

            public SerializeDataItem()
            {
                Value = 0;
            }

            public SerializeDataItem(byte[] key, ulong value)
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
            public SerializeDataItemList(int count)
            {
                Objects = new List<SerializeDataItem>(count);
            }
            public SerializeDataItemList()
            {
                Objects = new List<SerializeDataItem>();
            }

            public void Add(SerializeDataItem serializeDataItem)
            {
                Objects.Add(serializeDataItem);
            }
        }
    }
}