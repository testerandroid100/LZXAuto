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
        private ConcurrentDictionary<int, ulong> fileDict = new ConcurrentDictionary<int, ulong>();

        private string[] skipFileExtensions;
        private int threadQueueLength;

        private ulong totalDiskBytesLogical;
        private ulong totalDiskBytesPhysical;

        public Logger Logger { get; set; } = new Logger(LogLevel.General);

        public bool binaryDb = false;
        public bool skipSystem = true;

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

        public void Process(string path, string[] skipFileExtensionsArr)
        {
            skipFileExtensions = skipFileExtensionsArr ?? new string[] { };

            var driveCapacity = DriveUtils.GetDriveCapacity(path);
            var driveFreeSpace = DriveUtils.GetDriveFreeSpace(path);

            Logger.Log($"Starting new compressing session. LZXAuto version: {Assembly.GetEntryAssembly()?.GetName().Version}");
            Logger.Log($"Driver state:{Environment.NewLine} Capacity:{driveCapacity.GetMemoryString()}{Environment.NewLine}" +
                       $"Free space:{driveFreeSpace.GetMemoryString()}");
            Logger.Log($"Running in Administrator mode: {IsElevated}", 0);
            Logger.Log($"Starting path {path}", 0);

            var startTimeStamp = DateTime.Now;

            try
            {
                fileDict = LoadDictFromFile(dbFileName);

                Action<DirectoryInfo> processDirectory = null;
                processDirectory = (di) =>
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
                                    Interlocked.Increment(ref threadQueueLength);
                                    ThreadPool.QueueUserWorkItem(a => { ProcessFile(fi); });

                                    // Do not let queue length more items than MaxQueueLength
                                    while (threadQueueLength > maxQueueLength) Thread.Sleep(treadPoolWaitMs);
                                }
                                catch (Exception ex)
                                {
                                    Logger.Log(ex, fi);
                                }
                            }
                            catch (UnauthorizedAccessException)
                            {
                                Logger.Log($"Access failed to folder: {di.FullName}", 2, LogLevel.General);
                            }
                            catch (Exception ex)
                            {
                                Logger.Log(ex, di);
                            }

                        var directories = di.EnumerateDirectories("*", SearchOption.TopDirectoryOnly);
                        foreach (var nextDi in directories)
                            processDirectory(nextDi);

                    }
                    catch (Exception ex)
                    {
                        Logger.Log($"Unable to process folder {di.FullName}: {ex.Message}", 1, LogLevel.General);
                    }
                };

                var dirTop = new DirectoryInfo(path);
                processDirectory(dirTop);
            }
            catch (DirectoryNotFoundException DirNotFound)
            {
                Logger.Log(DirNotFound.Message);
            }
            catch (UnauthorizedAccessException unAuth)
            {
                Logger.Log(unAuth, unAuth.Message);
            }
            catch (PathTooLongException LongPath)
            {
                Logger.Log(LongPath.Message);
            }
            catch (Exception ex)
            {
                Logger.Log($"Other error: {ex.Message}");
            }
            finally
            {
                // Wait until all threads complete
                FinalizeThreadPool();

                var dirTop = new DirectoryInfo(path);
                DirectoryRemoveCompressAttr(dirTop);

                Logger.Log("Completed");

                SaveDictToFile(dbFileName, fileDict);

                var ts = DateTime.Now.Subtract(startTimeStamp);
                var totalFilesVisited = fileCountProcessedByCompactCommand + fileCountSkipByNoChange +
                                        fileCountSkippedByAttributes + fileCountSkippedByExtension;

                var spaceSavings = "-";
                if (compactCommandBytesRead > 0)
                    spaceSavings =
                        $"{(1 - (decimal)compactCommandBytesWritten / compactCommandBytesRead) * 100m:0.00}%";

                var compressionRatio = "-";
                if (compactCommandBytesWritten > 0)
                    compressionRatio = $"{(decimal)compactCommandBytesRead / compactCommandBytesWritten:0.00}";

                var spaceSaving = "-";
                if (totalDiskBytesLogical >0)
                    spaceSaving = $"{(1 - (decimal)totalDiskBytesPhysical / totalDiskBytesLogical) * 100m:0.00}%";

                var totalPhysicalRatio = "-";
                if (totalDiskBytesLogical > 0)
                    totalPhysicalRatio = $"{(decimal)totalDiskBytesLogical / totalDiskBytesPhysical:0.00}";

                Logger.Log(
                    $"Stats for this session: {Environment.NewLine}" +
                    $"Files skipped by attributes: {fileCountSkippedByAttributes}{Environment.NewLine}" +
                    $"Files skipped by extension: {fileCountSkippedByExtension}{Environment.NewLine}" +
                    $"Files skipped by no change: {fileCountSkipByNoChange}{Environment.NewLine}" +
                    $"Files processed by compact command line: {fileCountProcessedByCompactCommand}{Environment.NewLine}" +
                    $"Files in db: {fileDict?.Count ?? 0}{Environment.NewLine}" +
                    $"Files in db delta: {(fileDict?.Count ?? 0) - dictEntriesCount0}{Environment.NewLine}" +
                    $"Files visited: {totalFilesVisited}{Environment.NewLine}" +
                    $"{Environment.NewLine}" +
                    $"Bytes read: {compactCommandBytesRead.GetMemoryString()}{Environment.NewLine}" +
                    $"Bytes written: {compactCommandBytesWritten.GetMemoryString()}{Environment.NewLine}" +
                    $"Space savings bytes: {(compactCommandBytesRead - compactCommandBytesWritten).GetMemoryString()}{Environment.NewLine}" +
                    $"Space savings: {spaceSavings}{Environment.NewLine}" +
                    $"Compression ratio: {compressionRatio}{Environment.NewLine}{Environment.NewLine}" +
                    $"Disk stat:{Environment.NewLine}" +
                    $"Files logical size: {totalDiskBytesLogical.GetMemoryString()}{Environment.NewLine}" +
                    $"Files phisical size: {totalDiskBytesPhysical.GetMemoryString()}{Environment.NewLine}" +
                    $"Space savings: {spaceSaving}{Environment.NewLine}" +
                    $"Compression ratio: {totalPhysicalRatio}"
                    , 1, LogLevel.General);

                Logger.Log(
                    $"Perf stats:{Environment.NewLine}" +
                    $"Time elapsed[hh:mm:ss:ms]: {ts.Hours:00}:{ts.Minutes:00}:{ts.Seconds:00}:{ts.Milliseconds:00}{Environment.NewLine}" +
                    $"Compressed files per minute: {fileCountProcessedByCompactCommand / ts.TotalMinutes:0.00}{Environment.NewLine}" +
                    $"Files per minute: {totalFilesVisited / ts.TotalMinutes:0.00}", 1, LogLevel.General, false);
            }
        }

        private void ProcessFile(FileInfo fi)
        {
            try
            {
                var physicalSize1_Clusters = DriveUtils.GetPhysicalFileSize(fi.FullName);
                var logicalSize_Clusters = DriveUtils.GetDiskOccupiedSpace((ulong)fi.Length, fi.FullName);

                ThreadUtils.InterlockedAdd(ref totalDiskBytesLogical, logicalSize_Clusters);

                if (skipFileExtensions.Any(c => c == fi.Extension))
                {
                    ThreadUtils.InterlockedIncrement(ref fileCountSkippedByExtension);
                    ThreadUtils.InterlockedAdd(ref totalDiskBytesPhysical, physicalSize1_Clusters);
                    return;
                }

                if (skipSystem && fi.Attributes.HasFlag(FileAttributes.System))
                {
                    ThreadUtils.InterlockedIncrement(ref fileCountSkippedByAttributes);
                    ThreadUtils.InterlockedAdd(ref totalDiskBytesPhysical, physicalSize1_Clusters);
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
                    Logger.Log("", 4, LogLevel.Debug);


                    var filePathHash = fi.FullName.GetDeterministicHashCode();

                    if (fileDict.TryGetValue(filePathHash, out var dictFileSize) &&
                        dictFileSize == physicalSize1_Clusters)
                    {
                        Logger.Log(
                            $"Skipping file: '{fi.FullName}' because it has been visited already and its size ('{fi.Length.GetMemoryString()}') did not change",
                            1, LogLevel.Debug);
                        ThreadUtils.InterlockedIncrement(ref fileCountSkipByNoChange);
                        ThreadUtils.InterlockedAdd(ref totalDiskBytesPhysical, physicalSize1_Clusters);
                        return;
                    }

                    Logger.Log($"Compressing file {fi.FullName}", 1, LogLevel.Debug);
                    ThreadUtils.InterlockedIncrement(ref fileCountProcessedByCompactCommand);

                    var outPut = CompactCommand($"/c /exe:LZX {(useForceCompress ? "/f" : "")} \"{fi.FullName}\"");

                    var physicalSize2_Clusters = DriveUtils.GetPhysicalFileSize(fi.FullName);
                    fileDict[filePathHash] = physicalSize2_Clusters;

                    if (physicalSize1_Clusters > physicalSize2_Clusters)
                        Logger.Log(
                            $"DiskSize: {physicalSize1_Clusters} => {physicalSize2_Clusters}, fileName: {fi.FullName}",
                            0);

                    ThreadUtils.InterlockedAdd(ref compactCommandBytesRead, physicalSize1_Clusters);
                    ThreadUtils.InterlockedAdd(ref compactCommandBytesWritten, physicalSize2_Clusters);
                    ThreadUtils.InterlockedAdd(ref totalDiskBytesPhysical, physicalSize2_Clusters);

                    Logger.Log(outPut, 2, LogLevel.Debug);
                }
            }
            catch (UnauthorizedAccessException)
            {
                Logger.Log(
                    $"Error during processing: file: {fi.FullName}.{Environment.NewLine}Exception details: System.UnauthorizedAccessException");
            }
            catch (Exception ex)
            {
                Logger.Log(ex, fi);
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
                Logger.Log($"Removing NTFS compress flag on folder {dirTop.FullName} in favor of LZX compression", 1,
                    LogLevel.General);

                var outPut = CompactCommand($"/u \"{dirTop.FullName}\"");

                Logger.Log(outPut, 2, LogLevel.Debug);
            }
        }

        private string CompactCommand(string arguments)
        {
            var proc = new Process();
            proc.StartInfo.FileName = "compact";
            proc.StartInfo.Arguments = arguments;
            proc.StartInfo.UseShellExecute = false;
            proc.StartInfo.RedirectStandardOutput = true;

            Logger.Log(arguments, 1, LogLevel.Debug);

            proc.Start();
            try
            {
                proc.PriorityClass = ProcessPriorityClass.Idle;
            }
            catch (InvalidOperationException)
            {
                Logger.Log("Process Compact exited before setting its priority. Nothing to worry about.", 3,
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

        public void SaveDictToFile(string fileName, ConcurrentDictionary<int, ulong> concDict)
        {
            try
            {
                lock (ThreadUtils.lockObject)
                {
                    var items = new List<SerializeDataItem>(concDict.Count);
                    foreach (var key in concDict.Keys)
                        items.Add(new SerializeDataItem(key, concDict[key]));

                    using (FileStream writerFileStream = new FileStream(fileName, FileMode.Create, FileAccess.Write))
                    {
                        Logger.Log("Saving file...", 1, LogLevel.Debug);

                        if (binaryDb)
                        {
                            BinaryFormatter binaryFormatter = new BinaryFormatter();

                            var dict = concDict.ToDictionary(a => a.Key, b => b.Value);
                            binaryFormatter.Serialize(writerFileStream, dict);
                        }
                        else
                        {
                            var serializer = new XmlSerializer(typeof(List<SerializeDataItem>));
                            var ns = new XmlSerializerNamespaces();
                            ns.Add("", "");
                            serializer.Serialize(writerFileStream, items, ns);
                        }

                        Logger.Log($"File saved, dictCount: {concDict.Count}, fileSize: {writerFileStream.Length}", 1, LogLevel.Debug);

                        writerFileStream.Close();
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.Log($"Unable to save dic to file, {ex.Message}");
            }
        }

        public ConcurrentDictionary<int, ulong> LoadDictFromFile(string fileName)
        {
            var retVal = new ConcurrentDictionary<int, ulong>();

            if (File.Exists(fileName))
                try
                {
                    Logger.Log("Dictionary file found");

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
                                    retVal = new ConcurrentDictionary<int, ulong>((Dictionary<int, ulong>)dict);

                                }
                                catch (Exception) { }

                            }
                            else
                            {
                                var serial = new XmlSerializer(typeof(List<SerializeDataItem>), new[] { typeof(List<SerializeDataItem>) });

                                retVal.Clear();
                                try
                                {
                                    if (serial.Deserialize(readerFileStream) is List<SerializeDataItem> tempList)
                                        foreach (var item in tempList)
                                            retVal.TryAdd(item.Key, item.Value);
                                }
                                catch (Exception) { }
                            }

                            readerFileStream.Close();
                        }
                    }
                    
                    dictEntriesCount0 = (uint)retVal.Count;

                    Logger.Log($"Loaded from file ({dictEntriesCount0} entries)");
                }
                catch (Exception ex)
                {
                    Logger.Log($"Error during loading from file: {ex.Message}" +
                               $"{Environment.NewLine}Terminating.");

                    Environment.Exit(-1);
                }
            else
                Logger.Log("DB file not found");

            return retVal;
        }

        //private void FileSaveTimerCallback(object state)
        //{
        //    Logger.Log("Saving dictionary file...", 1, LogFlags.Debug);
        //    SaveDictToFile();
        //}

        public void Cancel()
        {
            Logger.Log("Terminating...", 3, LogLevel.General);
            cancelToken.Cancel();
        }

        private void FinalizeThreadPool()
        {
            // Disable file save timer callback
            //timer.Change(Timeout.Infinite, Timeout.Infinite);

            // Wait for thread pool to complete
            while (threadQueueLength > 0) Thread.Sleep(treadPoolWaitMs);
        }


        public class SerializeDataItem
        {
            public int Key;
            public ulong Value;

            public SerializeDataItem()
            {
                Key = 0;
                Value = 0;
            }

            public SerializeDataItem(int key, ulong value)
            {
                Key = key;
                Value = value;
            }
        }
    }
}