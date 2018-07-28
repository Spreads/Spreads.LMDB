// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

using NUnit.Framework;
using Spreads.LMDB.Interop;
using Spreads.Utils;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Spreads.Buffers;

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
            var env = new LMDBEnvironment("./Data/test_db");
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
                var env = new LMDBEnvironment("./Data/test_db" + i);
                env.MapSize = 10L * 1024 * 1024 * 1024;
                env.Open();
                var stat = env.GetStat();
            }

            await Task.Delay(60000000);
        }

        [Test]
        public async Task CouldWriteAsync()
        {
            var env = new LMDBEnvironment("./Data");
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
            });

            await env.Close();
        }

        [Test]
        public unsafe void CouldReserve()
        {
            var env = new LMDBEnvironment("./Data",
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
                });
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
            var env = new LMDBEnvironment("./Data",
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
            var env = new LMDBEnvironment("./Data");
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
            });

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
            var env = new LMDBEnvironment("./Data",
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
                    });
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
            var env = new LMDBEnvironment("./Data", DbEnvironmentFlags.WriteMap | DbEnvironmentFlags.NoSync);
            env.Open();

            var db = env.OpenDatabase("first_db", new DatabaseConfig(DbFlags.Create)).Result;

            var values = new byte[] { 1, 2, 3, 4 };

            var count = 10_000_000;

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
            var env = new LMDBEnvironment("./Data", DbEnvironmentFlags.WriteMap | DbEnvironmentFlags.NoSync);

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


        [Test, Explicit("long runnning")]
        public async Task CouldWriteDupfixedFromTwoThreads()
        {
            var env = new LMDBEnvironment("./Data", DbEnvironmentFlags.WriteMap | DbEnvironmentFlags.NoSync);

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
    }
}