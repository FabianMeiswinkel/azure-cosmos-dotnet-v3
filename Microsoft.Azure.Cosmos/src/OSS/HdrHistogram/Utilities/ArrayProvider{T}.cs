//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace HdrHistogram.Utilities
{
    using System;
    using System.Buffers;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Runtime.CompilerServices;
    using System.Text;
    using System.Threading;

    /// <summary>
    /// Provides a resource pool that enables reusing instances of arrays.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Renting and returning buffers with an <see cref="ArrayPool{T}"/> can increase performance
    /// in situations where arrays are created and destroyed frequently, resulting in significant
    /// memory pressure on the garbage collector.
    /// </para>
    /// <para>
    /// This class is thread-safe.  All members may be used by multiple threads concurrently.
    /// </para>
    /// </remarks>
    internal abstract class ArrayProvider<T>
    {
        private static readonly ConcurrentDictionary<int, ArrayProvider<T>> holder = 
            new ConcurrentDictionary<int, ArrayProvider<T>>();

        public static ArrayProvider<T> ForArrayOfLenght(int arrayLength)
        {
            return holder.GetOrAdd(arrayLength, (length) => new PooledArrayProvider(length));
        }

        public abstract T[] Rent();

        public abstract void Return(T[] array);

        internal sealed class PooledArrayProvider : ArrayProvider<T>
        {
            private readonly ConcurrentStack<T[]> stack = new ConcurrentStack<T[]>();
            private readonly int arrayLength;

            public PooledArrayProvider(int arrayLength)
            {
                this.arrayLength = arrayLength;
            }

            public override T[] Rent()
            {
                if (this.stack.TryPop(out T[] pooledArray))
                {
                    return pooledArray;
                }

                return new T[this.arrayLength];
            }

            public override void Return(T[] array)
            {
                if (array == null)
                {
                    throw new ArgumentNullException(nameof(array));
                }

                if (array.Length != this.arrayLength)
                {
                    throw new ArgumentOutOfRangeException(nameof(array));
                }

                this.stack.Push(array);
            }
        }
    }
}
