using System;
using System.Security.Cryptography;
using System.Text;

namespace LZXAutoEngine
{
    public static class Extensions
    {
        private static readonly object locker = new object();

        private static readonly MD5 md5Hash = MD5.Create();

        public static byte[] GenerateHashCode(this string str)
        {
            if (string.IsNullOrEmpty(str)) return Array.Empty<byte>();

            try
            {
                lock (locker)
                {
                    var byteArray = md5Hash.ComputeHash(Encoding.UTF8.GetBytes(str));
                    return byteArray;
                }
            }
            catch (Exception)
            {
                return Array.Empty<byte>();
            }
        }
    }
}