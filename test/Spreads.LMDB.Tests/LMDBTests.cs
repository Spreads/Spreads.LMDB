// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

using NUnit.Framework;
using Spreads.Buffers;
using Spreads.Serialization;
using Spreads.Utils;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Spreads.LMDB.Tests
{
    [TestFixture]
    public class LMDBTests
    {
        [Test]
        public async Task CouldCreateEnvironment()
        {
            // Assert.AreEqual(LMDBVersionInfo.Version, "LMDB 0.9.22: (March 21, 2018)");
            Console.WriteLine(LMDBVersionInfo.Version);
            var env = LMDBEnvironment.Create("./Data/test_db");
            env.Open();
            var stat = env.GetStat();
            Console.WriteLine("entries: " + stat.ms_entries);
            Console.WriteLine("MaxKeySize: " + env.MaxKeySize);
            await env.Close();
        }

        [Test, Ignore("")]
        public async Task CouldCreate500Environment()
        {
            Console.WriteLine(LMDBVersionInfo.Version);
            for (int i = 0; i < 200; i++)
            {
                var env = LMDBEnvironment.Create("./Data/test_db" + i);
                env.MapSize = 10L * 1024 * 1024 * 1024;
                env.Open();
                var stat = env.GetStat();
            }

            await Task.Delay(60000000);
        }

        [Test]
        public async Task CouldWriteAsync()
        {
            var env = LMDBEnvironment.Create("./Data");
            env.Open();
            var stat = env.GetStat();

            var db = await env.OpenDatabase("first_db", new DatabaseConfig(DbFlags.Create));

            var values = new byte[] { 1, 2, 3, 4 };

            await env.WriteAsync(txn =>
            {
                var key = new DirectBuffer(values);
                var value = new DirectBuffer(values);
                DirectBuffer value2 = default;

                using (var cursor = db.OpenCursor(txn))
                {
                    Assert.IsTrue(cursor.TryPut(ref key, ref value, CursorPutOptions.NoOverwrite));
                }

                using (var cursor = db.OpenCursor(txn))
                {
                    Assert.IsTrue(cursor.TryGet(ref key, ref value2, CursorGetOption.SetKey));
                }

                Assert.IsTrue(value2.Span.SequenceEqual(value.Span));

                return Task.CompletedTask;
            }, false, false);

            await env.Close();
        }

        [Test]
        public unsafe void CouldReserve()
        {
            var env = LMDBEnvironment.Create("./Data",
                DbEnvironmentFlags.WriteMap | DbEnvironmentFlags.NoSync);
            env.MapSize = 126 * 1024 * 1024;

            env.Open();

            var db = env.OpenDatabase("db_reserve", new DatabaseConfig(DbFlags.Create)).Result;

            var keyBytes = new byte[] { 1, 2, 3, 4 };
            var valueBytes = new byte[4096 * 2 - 16];

            var addresses = new long[100];
            var addresses2 = new long[addresses.Length];

            {
                env.Write(txn =>
                {
                    // var db2 = new Database("db_reserve", txn._impl, new DatabaseConfig(DbFlags.Create));

                    var addr = 0L;
                    var c = db.OpenCursor(txn);
                    for (int i = 0; i < addresses.Length; i++)
                    {
                        keyBytes[3] = (byte)i;
                        fixed (byte* keyPtr = &keyBytes[0], valPtr = &valueBytes[0])
                        {
                            var key = new DirectBuffer(keyBytes.Length, keyPtr);
                            var value = new DirectBuffer(valueBytes.Length, valPtr);

                            var stat1 = db.GetStat();

                            // c.Put(ref key, ref value, CursorPutOptions.ReserveSpace);
                            db.Put(txn, ref key, ref value, TransactionPutOptions.ReserveSpace);
                            // Console.WriteLine((long)value.mv_data);

                            if (i > 0)
                            {
                                addresses[i] = (long)value.Data - addr;
                                Console.WriteLine((long)value.Data + " - " + (long)value.Data % 4096 + " - " +
                                                  addresses[i]);
                            }

                            addr = (long)value.Data;
                        }
                    }

                    c.Dispose();
                    txn.Commit();
                }, false);
            }

            Console.WriteLine("---------------------------");

            env.Read(txn =>
            {
                // var c = db.OpenReadOnlyCursor(txn);

                var addr = 0L;
                for (int i = 0; i < addresses.Length; i++)
                {
                    keyBytes[3] = (byte)i;
                    fixed (byte* keyPtr = &keyBytes[0], valPtr = &valueBytes[0])
                    {
                        var key = new DirectBuffer(keyBytes.Length, keyPtr);
                        var value = new DirectBuffer(valueBytes.Length, valPtr);

                        // c.TryGet(CursorGetOption.SetKey, ref key, ref value);
                        db.TryGet(txn, ref key, out value);
                        //db.Put(txn, ref key, ref value, TransactionPutOptions.ReserveSpace);
                        // Console.WriteLine((long)value.mv_data);

                        if (i > 0)
                        {
                            addresses2[i] = (long)value.Data - addr;
                            Console.WriteLine((long)value.Data + " - " + (long)value.Data % 4096 + " - " + addresses2[i] + " - " + addresses[i]);
                            System.Runtime.CompilerServices.Unsafe.WriteUnaligned((void*)value.Data, i);
                        }
                        addr = (long)value.Data;
                    }
                }
                // c.Dispose();
            });
            var stat = env.GetStat();
            var dbstat = db.GetStat();
            Console.WriteLine("Oveflow pages: " + stat.ms_overflow_pages);
            env.Close().Wait();
        }

        [Test]
        public void CouldWrite()
        {
            var env = LMDBEnvironment.Create("./Data",
                DbEnvironmentFlags.WriteMap | DbEnvironmentFlags.NoSync);
            env.Open();
            var stat = env.GetStat();

            var db = env.OpenDatabase("first_db", new DatabaseConfig(DbFlags.Create)).Result;

            var values = new byte[] { 1, 2, 3, 4 };

            env.Write(txn =>
            {
                var key = new DirectBuffer(values);
                var value = new DirectBuffer(values);
                DirectBuffer value2 = default;

                using (var cursor = db.OpenCursor(txn))
                {
                    Assert.IsTrue(cursor.TryPut(ref key, ref value, CursorPutOptions.None));
                }

                using (var cursor = db.OpenCursor(txn))
                {
                    Assert.IsTrue(cursor.TryGet(ref key, ref value2, CursorGetOption.SetKey));
                }

                Assert.IsTrue(value2.Span.SequenceEqual(value.Span));
            });

            env.Close().Wait();
        }

        [Test, Explicit("long runnning")]
        public async Task CouldWriteAndReadProfileReadPath()
        {
            var env = LMDBEnvironment.Create("./Data");
            env.Open();

            var db = await env.OpenDatabase("first_db", new DatabaseConfig(DbFlags.Create));

            var values = new byte[] { 1, 2, 3, 4 };

            await env.WriteAsync(txn =>
            {
                var key = new DirectBuffer(values);
                var value = new DirectBuffer(values);
                DirectBuffer value2 = default;

                using (var cursor = db.OpenCursor(txn))
                {
                    Assert.IsTrue(cursor.TryPut(ref key, ref value, CursorPutOptions.None));
                }
                txn.Commit();
                return null;
            }, false, false);

            env.Read(txn =>
            {
                var key = new DirectBuffer(values);
                var value = new DirectBuffer(values);
                DirectBuffer value2 = default;

                var count = 1_00_000_000;

                //using (Benchmark.Run("Read path via cursor recreation", count))
                //{
                //    for (int i = 0; i < count; i++)
                //    {
                //        using (var cursor = db.OpenReadOnlyCursor(txn))
                //        {
                //            cursor.TryGet(CursorGetOption.SetKey, ref key, ref value2);
                //        }
                //    }
                //}

                using (Benchmark.Run("Read path reuse cursor", count))
                {
                    using (var cursor = db.OpenReadOnlyCursor(txn))
                    {
                        for (int i = 0; i < count; i++)
                        {
                            cursor.TryGet(ref key, ref value2, CursorGetOption.SetKey);
                        }
                    }
                }

                Benchmark.Dump();

                Assert.IsTrue(value2.Span.SequenceEqual(value.Span));

                return true;
            });

            await env.Close();
        }

        [Test, Explicit("long runnning")]
        public async Task CouldWriteAndReadProfileWriteAsyncPath()
        {
            var env = LMDBEnvironment.Create("./Data",
                // for any other config we have SQLite :)
                DbEnvironmentFlags.WriteMap | DbEnvironmentFlags.NoSync);
            env.Open();

            var db = await env.OpenDatabase("first_db", new DatabaseConfig(DbFlags.Create));

            var values = new byte[] { 1, 2, 3, 4 };

            var count = 1_000_000;

            using (Benchmark.Run("Write sync transactions", count))
            {
                for (var i = 0; i < count; i++)
                {
                    await env.WriteAsync(txn =>
                    {
                        var key = new DirectBuffer(values);
                        var value = new DirectBuffer(values);
                        DirectBuffer value2 = default;

                        using (var cursor = db.OpenCursor(txn))
                        {
                            cursor.TryPut(ref key, ref value, CursorPutOptions.None);
                        }

                        txn.Commit();
                        return null;
                    }, false, false);
                }
            }

            Benchmark.Dump();

            env.Read(txn =>
            {
                var key = new DirectBuffer(values);
                var value = new DirectBuffer(values);
                DirectBuffer value2 = default;

                using (var cursor = db.OpenReadOnlyCursor(txn))
                {
                    Assert.IsTrue(cursor.TryGet(ref key, ref value2, CursorGetOption.SetKey));
                }
                Assert.IsTrue(value2.Span.SequenceEqual(value.Span));

                return true;
            });

            await env.Close();
        }

        [Test, Explicit("long runnning")]
        public void CouldWriteAndReadProfileWriteSYNCPath()
        {
            var env = LMDBEnvironment.Create("./Data", DbEnvironmentFlags.WriteMap | DbEnvironmentFlags.NoSync);
            env.Open();

            var db = env.OpenDatabase("first_db", new DatabaseConfig(DbFlags.Create)).Result;

            var values = new byte[] { 1, 2, 3, 4 };

            var count = 1_000_000;

            // NB: Draining queue after benchmark ends, so fire and forget case only shows overhead of sending
            const bool fireAndForget = false;

            using (Benchmark.Run("Write sync transactions", count))
            {
                for (var i = 0; i < count; i++)
                {
                    env.Write(txn =>
                    {
                        var key = new DirectBuffer(values);
                        var value = new DirectBuffer(values);
                        DirectBuffer value2 = default;

                        using (var cursor = db.OpenCursor(txn))
                        {
                            cursor.TryPut(ref key, ref value, CursorPutOptions.None);
                        }

                        txn.Commit();
                    }, fireAndForget);
                }
            }

            Benchmark.Dump();

            env.Read(txn =>
            {
                var key = new DirectBuffer(values);
                var value = new DirectBuffer(values);
                DirectBuffer value2 = default;

                using (var cursor = db.OpenReadOnlyCursor(txn))
                {
                    Assert.IsTrue(cursor.TryGet(ref key, ref value2, CursorGetOption.SetKey));
                }
                Assert.IsTrue(value2.Span.SequenceEqual(value.Span));

                return true;
            });

            env.Close().Wait();
        }

        [Test, Explicit("long runnning")]
        public async Task CouldWriteDupfixed()
        {
            var env = LMDBEnvironment.Create("./Data", DbEnvironmentFlags.WriteMap | DbEnvironmentFlags.NoSync);

            env.MapSize = 100 * 1024 * 1024;
            env.Open();

            var db = await env.OpenDatabase("dupfixed_db",
                new DatabaseConfig(DbFlags.Create | DbFlags.IntegerDuplicates));

            //await db.Drop();
            //db = await env.OpenDatabase("dupfixed_db",
            //    new DatabaseConfig(DbFlags.Create | DbFlags.IntegerDuplicates));

            var valueHolder = new int[1];
            var mem = new Memory<int>(valueHolder);
            var handle = mem.Pin();
            var count = 1_000_000;

            using (Benchmark.Run("Write sync transactions", count))
            {
                for (var i = 1; i < count; i++)
                {
                    try
                    {
                        valueHolder[0] = i;
                        await db.PutAsync(0, i, TransactionPutOptions.AppendDuplicateData);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.ToString());
                    }
                    //await env.WriteAsync(txn =>
                    //{
                    //    valueHolder[0] = i;
                    //    var key = ((Memory<int>)keyHolder).AsMDBValUnsafe();
                    //    var value = ((Memory<int>)valueHolder).AsMDBValUnsafe();

                    //    db.Append(txn, ref key, ref value, true);

                    //    //using (var cursor = db.OpenCursor(txn))
                    //    //{
                    //    //    cursor.Append(ref key, ref value, true);
                    //    //}

                    //    txn.Commit();
                    //    return null;
                    //});
                }
            }
            handle.Dispose();

            Benchmark.Dump();

            await env.Close();
        }

        [Test]
        public async Task CouldDeleteDupSorted()
        {
            var env = LMDBEnvironment.Create("./Data", DbEnvironmentFlags.WriteMap | DbEnvironmentFlags.NoSync);

            env.MapSize = 100 * 1024 * 1024;
            env.Open();

            var db = await env.OpenDatabase("dupfixed_db",
                new DatabaseConfig(DbFlags.Create | DbFlags.IntegerDuplicates));
            db.Truncate().Wait();

            var count = 10;

            for (var i = 1; i <= count; i++)
            {
                try
                {
                    await db.PutAsync(0, i, TransactionPutOptions.AppendDuplicateData);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.ToString());
                }
            }

            using (var txn = env.BeginReadOnlyTransaction())
            {
                Assert.AreEqual(1, db.AsEnumerable<int, int>(txn).Count());
                foreach (var kvp in db.AsEnumerable<int,int>(txn))
                {
                    Console.WriteLine($"kvp: {kvp.Key} - {kvp.Value}");
                }

                Assert.AreEqual(10, db.AsEnumerable<int, int>(txn, 0).Count());
                foreach (var value in db.AsEnumerable<int, int>(txn, 0))
                {
                    Console.WriteLine("Key0 value: " + value);    
                }
            }


            env.Write(txn =>
            {
                db.Delete(txn, 0, 5);
                txn.Commit();
            });
            Console.WriteLine("AFTER DELETE SINGLE DUPSORT");
            using (var txn = env.BeginReadOnlyTransaction())
            {
                Assert.AreEqual(1, db.AsEnumerable<int, int>(txn).Count());
                foreach (var kvp in db.AsEnumerable<int, int>(txn))
                {
                    Console.WriteLine($"kvp: {kvp.Key} - {kvp.Value}");
                }

                Assert.AreEqual(9, db.AsEnumerable<int, int>(txn, 0).Count());
                foreach (var value in db.AsEnumerable<int, int>(txn, 0))
                {
                    Console.WriteLine("Key0 value: " + value);
                }
            }

            env.Write(txn =>
            {
                db.Delete(txn, 0);
                txn.Commit();
            });

            Console.WriteLine("AFTER DELETE ALL DUPSORT");
            using (var txn = env.BeginReadOnlyTransaction())
            {
                Assert.AreEqual(0, db.AsEnumerable<int, int>(txn).Count());
                foreach (var kvp in db.AsEnumerable<int, int>(txn))
                {
                    Console.WriteLine($"kvp: {kvp.Key} - {kvp.Value}");
                }

                Assert.AreEqual(0, db.AsEnumerable<int, int>(txn, 0).Count());
                foreach (var value in db.AsEnumerable<int, int>(txn, 0))
                {
                    Console.WriteLine("Key0 value: " + value);
                }
            }

            await env.Close();
        }

        [Test, Explicit("long runnning")]
        public async Task CouldWriteDupfixedFromTwoThreads()
        {
            var env = LMDBEnvironment.Create("./Data", DbEnvironmentFlags.WriteMap | DbEnvironmentFlags.NoSync);

            env.MapSize = 100 * 1024 * 1024;
            env.Open();

            var key = 0L;

            var count = 1_000_000;

            var db = env.OpenDatabase("dupfixed_db",
                new DatabaseConfig(DbFlags.Create | DbFlags.IntegerDuplicates)).Result;

            var t1 = Task.Run(() =>
            {
                using (Benchmark.Run("Write 1", count))
                {
                    for (var i = 1; i < count; i++)
                    {
                        try
                        {
                            db.PutAsync(0, Interlocked.Increment(ref key), TransactionPutOptions.NoDuplicateData).Wait();
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e.ToString());
                        }
                    }
                }
            });

            var t2 = Task.Run(() =>
            {
                using (Benchmark.Run("Write 2", count))
                {
                    for (var i = 1; i < count; i++)
                    {
                        try
                        {
                            db.PutAsync(0, Interlocked.Increment(ref key), TransactionPutOptions.NoDuplicateData).Wait();
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e.ToString());
                        }
                    }
                }
            });

            t1.Wait();
            t2.Wait();
            // handle.Dispose();

            Benchmark.Dump();

            await env.Close();
        }

        [Serialization(BlittableSize = 16)]
        public struct InPlaceUpdateable
        {
            public long Key;
            public long Value;
        }

        [Test, Explicit("long runnning")]
        public async Task CouldUpdateInplaceFromAbortedWriteTransactions()
        {
            var env = LMDBEnvironment.Create("./Data", DbEnvironmentFlags.WriteMap | DbEnvironmentFlags.NoSync);

            env.MapSize = 100 * 1024 * 1024;
            env.Open();

            var items = 1_000_000;
            var count = items * 100;
            var counts = new long[count];
            var changedPointers = new IntPtr[count];
            var finalPointers = new IntPtr[count];

            // one thread fills DB with new values, while another increments in-place values while it can and total
            // sum of

            var db = env.OpenDatabase("update_inplace",
                new DatabaseConfig(DbFlags.Create | DbFlags.IntegerKey)).Result;
            db.Truncate().Wait();

            var t1 = Task.Run(() =>
            {
                using (Benchmark.Run("Writer", items * 1000))
                {
                    for (var i = 1; i < items; i++)
                    {
                        try
                        {
                            var value = new InPlaceUpdateable()
                            {
                                Key = i,
                                Value = 0
                            };
                            db.PutAsync(i, value, TransactionPutOptions.AppendData | TransactionPutOptions.NoDuplicateData).Wait();
                            if (i % 1000 == 0)
                            {
                                // env.Sync(true);
                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e.ToString());
                        }
                    }
                }
            });

            var t2 = Task.Run(() =>
            {
                using (Benchmark.Run("Reader", count))
                {
                    var cnt = 0;
                    while (cnt < count)
                    {
                        DirectBuffer key = default;
                        DirectBuffer value = default;
                        var hasValue = false;
                        env.Read(txn =>
                        {
                            using (var c = db.OpenReadOnlyCursor(txn))
                            {
                                if (c.TryGet(ref key, ref value, CursorGetOption.First))
                                {
                                    hasValue = true;
                                    if (key.ReadInt32(0) != value.ReadInt64(0))
                                    {
                                        Assert.Fail($"Wrong keys: {key.ReadInt32(0)} vs {value.ReadInt64(0)}");
                                    }

                                    counts[value.ReadInt64(0)] = value.InterlockedCompareExchangeInt64(8, 1, 0);

                                    changedPointers[value.ReadInt64(0)] = value.Data;
                                    cnt++;
                                }
                            }
                            // txn.Abort();
                        });

                        var key1 = 0;

                        while (hasValue)
                        {
                            env.Read(txn =>
                            {
                                key1++;

                                if (db.TryGet(txn, ref key1, out value))
                                {
                                    counts[value.ReadInt64(0)] = value.InterlockedCompareExchangeInt64(8, 1, 0);
                                    changedPointers[value.ReadInt64(0)] = value.Data;
                                    cnt++;
                                }
                                else
                                {
                                    hasValue = false;
                                }

                                // txn.Abort();
                            });
                        }
                    }
                }
            });

            var t3 = Task.Run(() =>
            {
                using (Benchmark.Run("Reader", count))
                {
                    var cnt = 0;
                    while (cnt < count)
                    {
                        DirectBuffer key = default;
                        DirectBuffer value = default;
                        var hasValue = false;
                        env.Read(txn =>
                        {
                            using (var c = db.OpenReadOnlyCursor(txn))
                            {
                                if (c.TryGet(ref key, ref value, CursorGetOption.First))
                                {
                                    hasValue = true;
                                    if (key.ReadInt32(0) != value.ReadInt64(0))
                                    {
                                        Assert.Fail($"Wrong keys: {key.ReadInt32(0)} vs {value.ReadInt64(0)}");
                                    }
                                    counts[value.ReadInt64(0)] = value.InterlockedCompareExchangeInt64(8, 1, 0);
                                    changedPointers[value.ReadInt64(0)] = value.Data;
                                    cnt++;
                                }
                            }
                            // txn.Abort();
                        });

                        var key1 = 0;

                        while (hasValue)
                        {
                            env.Read(txn =>
                            {
                                key1++;

                                if (db.TryGet(txn, ref key1, out value))
                                {
                                    counts[value.ReadInt64(0)] = value.InterlockedCompareExchangeInt64(8, 1, 0);
                                    changedPointers[value.ReadInt64(0)] = value.Data;
                                    cnt++;
                                }
                                else
                                {
                                    hasValue = false;
                                }
                                // txn.Abort();
                            });
                        }
                    }
                }
            });
            t3.Wait();
            t2.Wait();
            t1.Wait();

            // handle.Dispose();

            long sum = 0;
            using (var txn = env.BeginReadOnlyTransaction())
            using (var c = db.OpenReadOnlyCursor(txn))
            {
                DirectBuffer key = default;
                DirectBuffer value = default;
                var cnt = 0;
                if (c.TryGet(ref key, ref value, CursorGetOption.First))
                {
                    sum += value.ReadInt64(8);
                    finalPointers[value.ReadInt64(0)] = value.Data;
                    cnt++;
                    while (c.TryGet(ref key, ref value, CursorGetOption.Next))
                    {
                        sum += value.ReadInt64(8);
                        finalPointers[value.ReadInt64(0)] = value.Data;
                        cnt++;
                    }
                }

                Console.WriteLine("COUNT: " + cnt);
            }

            Console.WriteLine("ACTUAL SUM: " + sum);
            Console.WriteLine("CALCULATED SUM: " + counts.Sum());
            Assert.AreEqual(counts.Sum(), sum);
            Benchmark.Dump();

            for (int i = 0; i < finalPointers.Length; i++)
            {
                if (changedPointers[i] != finalPointers[i])
                {
                    Console.WriteLine($"Pointers {i}: {changedPointers[i]} - {finalPointers[i]} - {changedPointers[i].ToInt64() - finalPointers[i].ToInt64()}");
                }
            }

            await env.Close();
        }

        [Test]
        public void CouldOpenMultipleEnvironmentsInTheSameProcess()
        {
            var env1 = LMDBEnvironment.Create("./Data/multiple_open_env");
            env1.Open();
            var env2 = LMDBEnvironment.Create("./Data/multiple_open_env");
            env2.Open();
            Assert.IsTrue(ReferenceEquals(env1, env2));
            Assert.IsTrue(env2.IsOpen);
            env2.Close().Wait();
            Assert.IsTrue(env1.IsOpen);
            var env3 = LMDBEnvironment.Create("./Data/multiple_open_env");
            Assert.IsTrue(ReferenceEquals(env1, env3));
            env1.Close().Wait();
            Assert.IsTrue(env3.IsOpen);
            env3.Close().Wait();
            Assert.IsFalse(env3.IsOpen);
        }
    }
}