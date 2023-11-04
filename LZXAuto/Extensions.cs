using System;
using System.Security.Cryptography;
using System.Text;

namespace LZXAutoEngine
{
    public static class Extensions
    {
        private static SHA256 sha256Hash = SHA256.Create();
        public static byte[] GenerateHashCode(this string str)
        {
            if (String.IsNullOrEmpty(str)) return new byte[32];

            byte[] data = sha256Hash.ComputeHash(Encoding.UTF8.GetBytes(str));
            return data;
        }
    }
}