using System;
using System.Runtime.CompilerServices;

namespace Spreads.LMDB
{
    internal static class Util
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void EnsureNoRefs<T>()
        {
            if (TypeHelper<T>.IsReferenceOrContainsReferences) Throw();
            void Throw()
            {
                throw new InvalidOperationException($"The type {typeof(T).Name} is a reference type or contains references.");
            }
        }
    }
}