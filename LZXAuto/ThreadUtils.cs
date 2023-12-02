﻿namespace LZXAutoEngine
{
    public static class ThreadUtils
    {
        public static object lockObject = new object();

        public static void InterlockedAdd(ref ulong variable, ref ulong value)
        {
            lock (lockObject)
            {
                variable += value;
            }
        }

        public static void InterlockedIncrement(ref uint variable)
        {
            lock (lockObject)
            {
                ++variable;
            }
        }
    }
}