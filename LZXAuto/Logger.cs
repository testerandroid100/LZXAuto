using System;
using System.IO;
using System.Text;

namespace LZXAutoEngine
{
    public class Logger
    {
        private const string logFileName = "Activity.log";
        private readonly object lockObject = new object();

        private string logMessage;
        private int chunkRecords;
        public const int chunkSize = 10;

        public Logger(LogLevel logLevel)
        {
            LogLevel = logLevel;
            chunkRecords = 0;
        }

        ~Logger()
        {
            if (chunkRecords == 0) return;

            File.AppendAllText(logFileName, logMessage);
            chunkRecords = 0;
            logMessage = "";
        }

        public LogLevel LogLevel { get; set; }

        public string TimeStamp => DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + " ";

        public void Log(Exception ex, DirectoryInfo di)
        {
            Log($"Error while processing directory: {di.FullName}, {ex}", 3);
        }

        public void Log(Exception ex, string customMessage)
        {
            Log($"Error message: {customMessage}.{Environment.NewLine}Exception details: {ex}", 3);
        }

        public void Log(Exception ex, FileInfo fi)
        {
            Log($"Error during processing: file: {fi.FullName}.{Environment.NewLine}Exception details: {ex}", 3);
        }

        public void Log(string str, int newLinePrefix = 1, LogLevel level = LogLevel.Info, bool showTimeStamp = true)
        {
            if ((int)LogLevel < (int)level) return;

            var sb = new StringBuilder();
            for (var i = 0; i < newLinePrefix; ++i) sb.AppendLine();

            if (!string.IsNullOrEmpty(str))
            {
                if (showTimeStamp)
                    sb.Append(TimeStamp);

                sb.Append(str);
            }

            var result = sb.ToString();
            Console.WriteLine(result);

            lock (lockObject)
            {
                ++chunkRecords;
                if (chunkRecords <= chunkSize)
                    logMessage += result;
                else
                {
                    File.AppendAllText(logFileName, logMessage);
                    chunkRecords = 0;
                    logMessage = "";
                }
            }
        }
    }


    public enum LogLevel
    {
        None = 0,
        General = 1,
        Info = 2,
        Debug = 3,
        Trace = 4
    }
}