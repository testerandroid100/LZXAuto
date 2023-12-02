using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security.Principal;
using System.Threading;

namespace LZXAutoEngine
{
    public class LZXAutoEngine
    {
        //private const int fileSaveTimerMs = (int)30e3; //30 seconds
        private const int treadPoolWaitMs = 200;

        private readonly CancellationTokenSource cancelToken = new CancellationTokenSource();
        private readonly int maxQueueLength = Environment.ProcessorCount * 16;

        private ulong compactCommandBytesRead;
        private ulong compactCommandBytesWritten;
        private uint totalFileCount;

        private uint fileCountProcessedByCompactCommand;
        private uint fileCountSkipByNoChange;
        private uint fileCountSkippedByAttributes;
        private uint fileCountSkippedByExtension;

        private string[] skipFileExtensions;
        private volatile int threadQueueLength;

        private ulong totalDiskBytesLogical;
        private ulong totalDiskBytesPhysical;

        public Logger logger { get; set; } = new Logger(LogLevel.General);

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
            logger.Log($"Starting path {path}{Environment.NewLine}");

            var startTimeStamp = DateTime.Now;
            logFileStatTime = startTimeStamp;

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

            // actualize drive stats
            Action<DirectoryInfo> addStatsFromDirectory = null;
            addStatsFromDirectory = di =>
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
                            if (fi.Length == 0)
                            {
                                ThreadUtils.InterlockedIncrement(ref totalFileCount);
                                continue;
                            }

                            ulong logicalSizeClusters = (ulong)fi.Length;
                            ulong physicalSizeClusters = DriveUtils.GetPhysicalFileSize(fi.FullName);

                            lock (logObject)
                            {
                                ThreadUtils.InterlockedIncrement(ref totalFileCount);
                                UpdateAndShowDiskStats(ref logicalSizeClusters, ref physicalSizeClusters);
                            }
                        }
                        catch (Exception ex)
                        {
                            logger.Log(ex, fi);
                        }
                    }

                    var directories = di.EnumerateDirectories("*", SearchOption.TopDirectoryOnly);
                    foreach (var nextDi in directories) addStatsFromDirectory(nextDi);
                }
                catch (Exception)
                {
                    // ignored
                }
            };

            var diskStatsTimeStamp = DateTime.Now;

            var driveRoot = new DirectoryInfo(path.Substring(0, 2) + "\\");
            addStatsFromDirectory(driveRoot);

            ts = DateTime.Now.Subtract(diskStatsTimeStamp);
            logger.Log(
                $"Update disk stats completed in [hh:mm:ss:ms]: {ts.Hours:00}:{ts.Minutes:00}:{ts.Seconds:00}:{ts.Milliseconds:00}{Environment.NewLine}",
                2);
            logger.Flush();

            logger.Log("All operations completed.", 2);

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
                $"Files on disk: {totalFileCount}{Environment.NewLine}" +
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
                , 2, LogLevel.General);

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
                if (fi.Length <= 0)
                {
                    ThreadUtils.InterlockedIncrement(ref totalFileCount);
                    return;
                }

                var prevPhysicalSizeClusters = DriveUtils.GetPhysicalFileSize(fi.FullName);
                if (prevPhysicalSizeClusters == 0) return;

                var logicalSizeClusters = DriveUtils.GetDiskOccupiedSpace((ulong)fi.Length, fi.FullName);

                if (skipFileExtensions.Any(c => c == fi.Extension))
                {
                    lock (logObject)
                    {
                        ThreadUtils.InterlockedIncrement(ref totalFileCount);
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
                        ThreadUtils.InterlockedIncrement(ref totalFileCount);
                        ThreadUtils.InterlockedIncrement(ref fileCountSkippedByAttributes);
                        logger.Log($"Skip file '{fi.FullName}' by system flag.", 1, LogLevel.Debug);

                        UpdateAndShowDiskStats(ref logicalSizeClusters, ref prevPhysicalSizeClusters);
                    }

                    return;
                }

                //logger.Log("", 2, LogLevel.Debug);
                if (prevPhysicalSizeClusters != logicalSizeClusters && !fi.Attributes.HasFlag(FileAttributes.Compressed))
                {
                    lock (logObject)
                    {
                        ThreadUtils.InterlockedIncrement(ref totalFileCount);
                        ThreadUtils.InterlockedIncrement(ref fileCountSkipByNoChange);
                        logger.Log(
                            $"Skip file '{fi.FullName}' because it has been visited already and its size ('{fi.Length.GetMemoryString()}') did not change",
                            1, LogLevel.Debug);

                        UpdateAndShowDiskStats(ref logicalSizeClusters, ref prevPhysicalSizeClusters);
                    }

                    return;
                }

                var useForceCompress = false;
                if (fi.Attributes.HasFlag(FileAttributes.Compressed))
                {
                    File.SetAttributes(fi.FullName, fi.Attributes & ~FileAttributes.Compressed);
                    useForceCompress = true;
                }

                lock (logObject)
                {
                    logger.Log($"Compressing file {fi.FullName}", 1, LogLevel.Debug);
                }

                ThreadUtils.InterlockedIncrement(ref fileCountProcessedByCompactCommand);

                var outPut = CompactCommand($"/c /exe:LZX {(useForceCompress ? "/f" : "")} \"{fi.FullName}\"");

                var currentPhysicalSizeClusters = DriveUtils.GetPhysicalFileSize(fi.FullName);
                lock (logObject)
                {
                    ThreadUtils.InterlockedAdd(ref compactCommandBytesRead, ref prevPhysicalSizeClusters);
                    ThreadUtils.InterlockedAdd(ref compactCommandBytesWritten, ref currentPhysicalSizeClusters);
                    ThreadUtils.InterlockedIncrement(ref totalFileCount);
                    logger.Log(
                        $"File size stats: {prevPhysicalSizeClusters} => {currentPhysicalSizeClusters}, fileName: {fi.FullName}");

                    UpdateAndShowDiskStats(ref logicalSizeClusters, ref prevPhysicalSizeClusters);
                }
            }
            catch (UnauthorizedAccessException)
            {
                lock (logObject)
                {
                    logger.Log(
                        $"Error during processing file: {fi.FullName}.{Environment.NewLine}Exception details: System.UnauthorizedAccessException");
                }
            }
            catch (PathTooLongException)
            {
                lock (logObject)
                {
                    logger.Log(
                        $"Error during processing file: {fi.FullName}.{Environment.NewLine}Exception details: System.IO.PathTooLongException");
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
            logger.ShowDiskStats(totalFileCount, ref totalDiskBytesPhysical, ref totalDiskBytesLogical);
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
    }
}