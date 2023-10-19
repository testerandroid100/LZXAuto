using System;
using System.IO;
using System.Runtime.InteropServices;

namespace LZXAutoEngine
{
    public static class DriveUtils
    {
        private static readonly string[] BinaryPrefix = { "bytes", "KB", "MB", "GB", "TB" }; // , "PB", "EB", "ZB", "YB"

        private static uint lpSectorsPerCluster, lpBytesPerSector, lpNumberOfFreeClusters, lpTotalNumberOfClusters;
        
        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Auto)]
        private static extern bool GetDiskFreeSpace(string lpRootPathName, out uint lpSectorsPerCluster,
            out uint lpBytesPerSector, out uint lpNumberOfFreeClusters, out uint lpTotalNumberOfClusters);

        private static void ReadDiskParams(string path)
        {
            GetDiskFreeSpace(Path.GetPathRoot(path), out lpSectorsPerCluster, out lpBytesPerSector,
                out lpNumberOfFreeClusters, out lpTotalNumberOfClusters);
        }

        public static long GetDriveFreeSpace(string path)
        {
            var d = new DriveInfo(Path.GetPathRoot(path));
            return d.AvailableFreeSpace;
        }

        public static long GetDriveCapacity(string path)
        {
            var d = new DriveInfo(Path.GetPathRoot(path));
            return d.TotalSize;
        }

        public static ulong GetDiskOccupiedSpace(ulong logicalFileSize, string path)
        {
            if (lpBytesPerSector == 0)
                ReadDiskParams(path);

            ulong clustersize = lpBytesPerSector * lpSectorsPerCluster;
            return clustersize * ((logicalFileSize + clustersize - 1) / clustersize);
        }

        [DllImport("kernel32.dll")]
        private static extern uint GetCompressedFileSizeW([In] [MarshalAs(UnmanagedType.LPWStr)] string lpFileName,
            [Out] [MarshalAs(UnmanagedType.U4)] out uint lpFileSizeHigh);

        public static ulong GetPhysicalFileSize(string fileName)
        {
            var low = GetCompressedFileSizeW(fileName, out var high);
            var error = Marshal.GetLastWin32Error();
            if (low == 0xFFFFFFFF && error != 0)
                return 0;
            //throw new Win32Exception(error, "Error while getting physical file size");
            return ((ulong)high << 32) + low;
        }

        public static string GetMemoryString(this long bytes)
        {
            var index = 0;
            double value = bytes;
            string text;
            do
            {
                text = value.ToString("0.0") + " " + BinaryPrefix[index];
                value /= 1024;
                index++;
            } while (Math.Floor(value) > 0 && index < BinaryPrefix.Length);

            return text;
        }

        public static string GetMemoryString(this ulong bytes)
        {
            return GetMemoryString((long)bytes);
        }
    }
}