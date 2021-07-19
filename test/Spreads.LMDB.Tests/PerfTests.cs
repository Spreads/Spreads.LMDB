// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

using NUnit.Framework;
using Spreads.Buffers;
using Spreads.Utils;
using System;
using System.IO;
using System.Runtime.CompilerServices;

namespace Spreads.LMDB.Tests
{
    [TestFixture]
    public class PerfTests
    {
        [Test, Explicit("long running")]
        public void SimpleWriteReadBenchmark()
        {
#pragma warning disable 618
            Settings.DoAdditionalCorrectnessChecks = false;
#pragma warning restore 618

            var count = 1_00_000;
            var rounds = 1;
            var extraReadRounds = 10;
            var path = "./data/benchmark";
            if (Directory.Exists(path))
            {
                Directory.Delete(path, true);
            }

            var dirS = Path.Combine(path, "Spreads");
            Directory.CreateDirectory(dirS);

            var envS = LMDBEnvironment.Create(dirS, LMDBEnvironmentFlags.NoSync | LMDBEnvironmentFlags.NoLock,
                disableAsync: true);
            envS.MaxDatabases = 10;
            envS.MapSize = 256 * 1024 * 1024;
            envS.Open();
            envS.TouchSpace(500);

            Console.WriteLine("USED SIZE: " + envS.UsedSize);

            var dbS = envS.OpenDatabase("SimpleWrite", new DatabaseConfig(DbFlags.Create | DbFlags.IntegerKey));

            for (int r = 0; r < rounds; r++)
            {
                using (Benchmark.Run("Spreads Write (K)", count * 1000, true))
                {
                    for (long i = r * count; i < (r + 1) * count; i++)
                    {
                        using (var tx = envS.BeginTransaction(TransactionBeginFlags.ReadWrite))
                        {
                            dbS.Put(tx, i, i, TransactionPutOptions.AppendData);
                            tx.Commit();
                        }
                    }
                }

                using (Benchmark.Run("Spreads Read", count * extraReadRounds, true))
                {
                    for (long j = 0; j < extraReadRounds; j++)
                    {
                        for (long i = r * count; i < (r + 1) * count; i++)
                        {
                            using (var tx = envS.BeginReadOnlyTransaction())
                            {
                                dbS.TryGet(tx, ref i, out long val);
                                if (val != i)
                                {
                                    Assert.Fail();
                                }
                            }
                        }
                    }
                }
            }

            dbS.Dispose();
            envS.Close();

            Benchmark.Dump("SimpleWrite/Read 1M longs");
        }

        //public unsafe long TouchSpace(int megabytes = 0)
        //{
        //    EnsureOpened();
        //    int size = 0;
        //    if (megabytes == 0)
        //    {
        //        var used = UsedSize;
        //        size = (int)((MapSize - used) / 2);
        //        if (size == 0)
        //        {
        //            return used;
        //        }
        //    }
        //    else
        //    {
        //        size = megabytes * 1024 * 1024;
        //    }
        //    if (megabytes * 1024 * 1024 > MapSize)
        //    {
        //        throw new InvalidOperationException("Canno touch space above MapSize");
        //    }
        //    var db = OpenDatabase("__touch_space___", new DatabaseConfig(DbFlags.Create));
        //    using (var txn = BeginTransaction(TransactionBeginFlags.NoSync))
        //    {
        //        var key = 0;
        //        var keyPtr = Unsafe.AsPointer(ref key);
        //        var key1 = new DirectBuffer(TypeHelper<int>.FixedSize, (byte*)keyPtr);
        //        DirectBuffer value = new DirectBuffer(size, (byte*)IntPtr.Zero);
        //        db.Put(txn, ref key1, ref value, TransactionPutOptions.ReserveSpace);
        //        txn.Commit();
        //    }

        //    using (var txn = BeginTransaction(TransactionBeginFlags.NoSync))
        //    {
        //        db.Truncate(txn);
        //        db.Drop(txn);
        //    }
        //    db.Dispose();
        //    return UsedSize;
        //}

        [Test, Explicit("long running")]
        public unsafe void SimpleWriteReadBatchedBenchmark()
        {
#pragma warning disable 618
            Settings.DoAdditionalCorrectnessChecks = false;
#pragma warning restore 618
            var count = TestUtils.GetBenchCount(TestUtils.InDocker ? 100_000 : 1_000_000, 100_000);
            var rounds = 10;
            var extraRounds = 10;

            var path = "./data/benchmarkbatched";
            if (Directory.Exists(path))
            {
                Directory.Delete(path, true);
            }

            var dirS = Path.Combine(path, "Spreads");
            var dirK = Path.Combine(path, "KdSoft");
            Directory.CreateDirectory(dirS);
            Directory.CreateDirectory(dirK);

            var envS = LMDBEnvironment.Create(dirS, LMDBEnvironmentFlags.MapAsync, disableAsync: true);
            envS.MaxDatabases = 10;
            envS.MapSize = 256 * 1024 * 1024;
            envS.Open();
            // envS.TouchSpace(100);
            Console.WriteLine("USED SIZE: " + envS.UsedSize);
            var dbS = envS.OpenDatabase("SimpleWrite", new DatabaseConfig(DbFlags.Create | DbFlags.IntegerKey));

            for (int i = 0; i < 2; i++)
            {
                using (var txn = envS.BeginTransaction(TransactionBeginFlags.NoSync))
                {
                    var key = 0L;
                    var keyPtr = Unsafe.AsPointer(ref key);
                    var key1 = new DirectBuffer(TypeHelper<int>.FixedSize, (nint)keyPtr);
                    DirectBuffer value = new DirectBuffer(32 * 1024 * 1024, (nint)1);
                    // Note: DirectBuffer used to have an unsafe ctor that accepts null for data,
                    // here we emulate this behavior (the layout is fixed and won't change because it matches MDB_VAL):
                    Unsafe.AddByteOffset(ref Unsafe.As<DirectBuffer, nint>(ref Unsafe.AsRef(in value)), (nuint)IntPtr.Size) = IntPtr.Zero;
                    
                    dbS.Put(txn, ref key1, ref value, TransactionPutOptions.ReserveSpace);
                    txn.Commit();
                }

                using (var txn = envS.BeginTransaction(TransactionBeginFlags.NoSync))
                {
                    var key = 0L;
                    var keyPtr = Unsafe.AsPointer(ref key);
                    var key1 = new DirectBuffer(TypeHelper<int>.FixedSize, (nint)keyPtr);
                    dbS.Delete(txn, ref key1);
                    txn.Commit();
                }
            }

            // var garbage1 = new byte[1];

            for (int r = 0; r < rounds; r++)
            {
                using (Benchmark.Run("Spreads Write", count, false))
                {
                    var tx = envS.BeginTransaction();
                    // using (tx)
                    {
                        for (long i = r * count; i < (r + 1) * count; i++)
                        {
                            dbS.Put(tx, i, i, TransactionPutOptions.AppendData);
                            if (i % 10000 == 0)
                            {
                                tx.Commit();
                                tx.Dispose();
                                tx = envS.BeginTransaction();
                            }
                        }

                        tx.Commit();
                        tx.Dispose();
                    }
                }

                using (Benchmark.Run("Spreads Read", count * extraRounds, false))
                {
                    using (var tx = envS.BeginReadOnlyTransaction())
                    {
                        for (long j = 0; j < extraRounds; j++)
                        {
                            for (long i = r * count; i < (r + 1) * count; i++)
                            {
                                dbS.TryGet(tx, ref i, out long val);
                                if (val != i)
                                {
                                    Assert.Fail();
                                }
                            }
                        }
                    }
                }
            }

            dbS.Dispose();
            envS.Close();

            Benchmark.Dump("SimpleBatchedWrite/Read 10x1M longs");
            // Console.WriteLine(garbage1[0]);
        }

        [Test, Explicit("long running")]
        public void DiskSyncWriteRead()
        {
            var count = 5_000;
            var rounds = 1;

            var path = "./data/benchmark";
            if (Directory.Exists(path))
            {
                Directory.Delete(path, true);
            }

            var dirS = Path.Combine(path, "Spreads");
            var dirK = Path.Combine(path, "KdSoft");
            Directory.CreateDirectory(dirS);
            Directory.CreateDirectory(dirK);

            var envS = LMDBEnvironment.Create(dirS, LMDBEnvironmentFlags.WriteMap | LMDBEnvironmentFlags.NoMetaSync,
                disableAsync: true);
            envS.MaxDatabases = 10;
            envS.MapSize = 16 * 1024 * 1024;
            envS.Open();
            var dbS = envS.OpenDatabase("SimpleWrite", new DatabaseConfig(DbFlags.Create | DbFlags.IntegerKey));

            for (int r = 0; r < rounds; r++)
            {
                using (Benchmark.Run("Spreads Write", count * 1_000_000, true))
                {
                    for (long i = r * count; i < (r + 1) * count; i++)
                    {
                        using (var tx = envS.BeginTransaction())
                        {
                            dbS.Put(tx, i, i, TransactionPutOptions.AppendData);
                            tx.Commit();
                        }
                    }
                }

                using (Benchmark.Run("Spreads Read", count, true))
                {
                    for (long i = r * count; i < (r + 1) * count; i++)
                    {
                        using (var tx = envS.BeginReadOnlyTransaction())
                        {
                            dbS.TryGet(tx, ref i, out long val);
                            if (val != i)
                            {
                                Assert.Fail();
                            }
                        }
                    }
                }
            }
            dbS.Dispose();
            envS.Close();
            Benchmark.Dump("Writes in single OPS");
        }
    }
}
