using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using LZXAutoEngine;
using ZeroDep;

namespace LZXAuto
{
    internal class LZXAuto
    {
        private const string TaskScheduleName = "LZXAuto";
        private const string TaskScheduleTemplateFileName = "LZXAutoTask.xml";

        private static readonly LZXAutoEngine.LZXAutoEngine CompressorEngine = new LZXAutoEngine.LZXAutoEngine();

        private static void Main(string[] args)
        {
            var thisprocessname = Process.GetCurrentProcess().ProcessName;
            if (Process.GetProcesses().Count(p => p.ProcessName == thisprocessname) > 1)
            {
                CompressorEngine.Logger.Log("Another instance is already running. Exiting...", 2, LogLevel.General);
                return;
            }

            AppDomain.CurrentDomain.ProcessExit += CurrentDomain_ProcessExit;
            Console.CancelKeyPress += ConsoleTerminateHandler;

            var commandLine = string.Empty;
            if (args != null) commandLine = string.Join(" ", args);

            // Parse help option
            if (args != null && (args.Length == 0 || args.Contains("/?") || args.Contains("/help")))
            {
                Console.WriteLine($@"
Automatically compress files to NTFS LZX compression with minimal disk write cycles.
                
Syntax: LZXAuto [/log:mode] [/resetDb] [/scheduleOn] [/scheduleOff] [/binaryDB] [/? | /help] [filePath]

Options:

/log: [None, General, Info, Debug] - log level. Default value: Info
None    - nothing is outputted
General - Session start / end timestamp, skipped folders
Info    - General + statistics about current session
Debug   - Info + information about every file

/resetDb - resets db. On next run, all files will be traversed by Compact command.

/scheduleOn - enables Task Scheduler entry to run LZXAuto when computer is idle for 10 minutes. Task runs daily.

/scheduleOff - disables Task Scheduler entry

/binaryDB - binary database (default - xml)

/skipSystem:off - process system files (default - on)

/? or /help - displays this help screen

filePath - root path to start. All subdirectories will be traversed. Default is root of current drive, like c:\.

Description:
Windows 10 extended NTFS compression with LZX alghorithm. 
Files compressed with LZX can be opened like any other file because the uncompressing operation is transparent.
Compressing files with LZX is CPU intensive and thus is not being done automatically. When file is updated, it will be saved in uncompressed state.
To keep the files compressed, windows Compact command needs to be re-run. This can be done with Task Scheduler.

There is a catch with SSD drives though.
When Compact command is being run on file already LZX-compressed, it will not try to recompress it.
However, if file is not compressible (like .jpg image), Compact will try to recompress it every time, writing temp data to disk.

This is an issue on SSD drives, because of limited write cycles.
LZXAuto keeps record of file name and its last seen size. If the file has not changed since last LZXAuto run, it will be skipped. 
This saves SSD write cycles and also speeds up processing time, as on second run only newly updated / inserted files are processed.

If folder is found with NTFS compression enabled, after processing it will be marked as non-compressed. 
This is because LZX-compression does not use NTFS Compressed attribute.

Iterating through files is multithreaded, one file per CPU logical core.
For larger file accessibility, this command should be run with Adminstrator priviledges.

Typical use:
LZXAuto /scheduleOn c:\ 

Version number: {Assembly.GetEntryAssembly()?.GetName().Version}
");

                return;
            }

            // Parse resetDb option
            if (args != null && args.Contains("/resetDb", StringComparer.InvariantCultureIgnoreCase))
            {
                CompressorEngine.ResetDb();
                return;
            }

            CompressorEngine.Logger.LogLevel = LogLevel.Info;

            // Parse log level option, like: /q:general
            if (!string.IsNullOrEmpty(commandLine))
            {
                var rx = new Regex(@"/log:(?<mode>(\s*\w+\s*[,]{0,1})*)(?![:/])", RegexOptions.IgnoreCase);
                var match = rx.Match(commandLine);
                if (match.Success)
                {
                    CompressorEngine.Logger.LogLevel = LogLevel.None;

                    var modeStr = match.Groups["mode"]?.Value;
                    if (modeStr != null)
                    {
                        var modeArr = modeStr.Replace(" ", string.Empty).Split(',');

                        foreach (var modeVal in modeArr.Where(a => !string.IsNullOrEmpty(a)))
                        {
                            if (!Enum.TryParse(modeVal, true, out LogLevel logL))
                            {
                                Console.WriteLine($"Unrecognised log level value: {modeVal}");
                                return;
                            }

                            CompressorEngine.Logger.LogLevel = logL;
                        }
                    }
                }
            }

            // Parse path option
            var commandLineRequestedPath = Path.GetPathRoot(Assembly.GetEntryAssembly()?.Location);
            if (args != null)
            {
                foreach (var arg in args)
                {
                    var rx = new Regex(@"[a-z]:\\", RegexOptions.IgnoreCase);
                    if (!string.IsNullOrEmpty(rx.Match(arg).Value))
                    {
                        commandLineRequestedPath = arg;
                        if (!commandLineRequestedPath.EndsWith("\\")) commandLineRequestedPath += "\\";
                    }
                }

                // Parse scheduleOn option
                if (args.Contains("/scheduleOn", StringComparer.InvariantCultureIgnoreCase))
                {
                    var currentProcess = Process.GetCurrentProcess();
                    if (currentProcess.MainModule != null)
                    {
                        var currentProcessPath = currentProcess.MainModule.FileName;

                        var requestedPath = commandLineRequestedPath;
                        if (string.IsNullOrEmpty(requestedPath))
                            requestedPath = Path.GetPathRoot(currentProcessPath);

                        var newCommandNode = $"<Command>\"{currentProcessPath}\"</Command>";
                        var newArgumentsNode = $"<Arguments>{requestedPath}</Arguments>";
                        var newWorkingDirectoryNode =
                            $"<WorkingDirectory>{Path.GetDirectoryName(currentProcessPath)}</WorkingDirectory>";

                        ReplaceTextInFile(TaskScheduleTemplateFileName, "<Command></Command>", newCommandNode);
                        ReplaceTextInFile(TaskScheduleTemplateFileName, "<Arguments></Arguments>", newArgumentsNode);
                        ReplaceTextInFile(TaskScheduleTemplateFileName, "<WorkingDirectory></WorkingDirectory>",
                            newWorkingDirectoryNode);

                        try
                        {
                            var proc = new Process();
                            proc.StartInfo.FileName = "schtasks";
                            proc.StartInfo.Arguments =
                                $"/Create /XML {TaskScheduleTemplateFileName} /tn {TaskScheduleName}";
                            proc.StartInfo.UseShellExecute = false;
                            proc.StartInfo.RedirectStandardOutput = true;
                            proc.Start();
                            proc.WaitForExit();
                            var exitCode = proc.ExitCode;
                            Console.WriteLine(exitCode == 0
                                ? "Schedule initialized"
                                : "Schedule initialization failed");

                            proc.Close();
                        }
                        finally
                        {
                            ReplaceTextInFile(TaskScheduleTemplateFileName, newCommandNode, "<Command></Command>");
                            ReplaceTextInFile(TaskScheduleTemplateFileName, newArgumentsNode,
                                "<Arguments></Arguments>");
                            ReplaceTextInFile(TaskScheduleTemplateFileName, newWorkingDirectoryNode,
                                "<WorkingDirectory></WorkingDirectory>");
                        }
                    }

                    return;
                }

                // Parse scheduleOff option
                if (args.Contains("/scheduleOff", StringComparer.InvariantCultureIgnoreCase))
                {
                    var proc = new Process();
                    proc.StartInfo.FileName = "schtasks";
                    proc.StartInfo.Arguments = $"/Delete /tn {TaskScheduleName} /f";
                    proc.StartInfo.UseShellExecute = false;
                    proc.StartInfo.RedirectStandardOutput = true;
                    proc.Start();
                    proc.WaitForExit();
                    var exitCode = proc.ExitCode;
                    Console.WriteLine(exitCode == 0 ? "Schedule deleted" : "Schedule deletion failed");

                    proc.Close();

                    return;
                }

                if (args.Contains("/binaryDB", StringComparer.InvariantCultureIgnoreCase))
                {
                    CompressorEngine.binaryDb = true;
                }

                if (args.Contains("/skipSystem:off", StringComparer.InvariantCultureIgnoreCase))
                {
                    CompressorEngine.skipSystem = false;
                }
            }

            string[] skipFileExtensions;
            try
            {
                // Read config file
                var file = File.ReadAllText("LZXAutoConfig.json");

                var jsonDict = Json.Deserialize<Dictionary<string, dynamic>>(file);

                var extList = new List<string>();
                var extArray = jsonDict["skipFileExtensions"];
                foreach (var ext in extArray)
                {
                    string value = Convert.ToString(ext);
                    extList.Add(value);
                }

                skipFileExtensions = extList.ToArray();
            }
            catch (Exception ex)
            {
                CompressorEngine.Logger.Log(ex, "Could not parse LZXAutoConfig.json");
                return;
            }

            CompressorEngine.Process(commandLineRequestedPath, skipFileExtensions);
        }

        private static void CurrentDomain_ProcessExit(object sender, EventArgs e)
        {
            //compressorEngine.Cancel();
        }

        private static void ConsoleTerminateHandler(object sender, ConsoleCancelEventArgs args)
        {
            args.Cancel = false;
            CompressorEngine.Cancel();
        }

        private static void ReplaceTextInFile(string fileName, string sourceText, string replacementText)
        {
            var text = File.ReadAllText(fileName);
            text = text.Replace(sourceText, replacementText);
            File.WriteAllText(fileName, text);
        }
    }
}