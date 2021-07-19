// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

using NUnit.Framework;
using Spreads.Buffers;
using Spreads.Utils;
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
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
            //var x = Marshal.AllocHGlobal(128);
            //var y = Marshal.AllocHGlobal(128);

            //Spreads.Native.Compression.shuffle((IntPtr)8, (IntPtr)16, (byte*)x, (byte*)y);

            // Assert.AreEqual(LMDBVersionInfo.Version, "LMDB 0.9.22: (March 21, 2018)");
            Console.WriteLine(LMDBVersionInfo.Version);
            var env = LMDBEnvironment.Create("./Data/CouldCreateEnvironment");
            env.Open();
            var stat = env.GetStat();
            Console.WriteLine("entries: " + stat.ms_entries);
            Console.WriteLine("MaxKeySize: " + env.MaxKeySize);
            Console.WriteLine("ReaderCheck: " + env.ReaderCheck());
            env.Close();
        }

        [Test]
        public async Task CouldCreateEnvironmentWithCyrillicPath()
        {
            // Assert.AreEqual(LMDBVersionInfo.Version, "LMDB 0.9.22: (March 21, 2018)");
            Console.WriteLine(LMDBVersionInfo.Version);
            var env = LMDBEnvironment.Create("./Data/CouldCreateEnvironment/МояПапка");
            env.Open();
            var stat = env.GetStat();
            Console.WriteLine("entries: " + stat.ms_entries);
            Console.WriteLine("MaxKeySize: " + env.MaxKeySize);
            Console.WriteLine("ReaderCheck: " + env.ReaderCheck());
            env.Close();
        }

        [Test]
        public async Task CouldCreateEnvironmentWithFullPath()
        {
            var path = Path.GetFullPath(Path.Combine(TestUtils.GetPath(), "subpath"));
            // Assert.AreEqual(LMDBVersionInfo.Version, "LMDB 0.9.22: (March 21, 2018)");
            Console.WriteLine(LMDBVersionInfo.Version);
            var env = LMDBEnvironment.Create(path);
            env.Open();
            var stat = env.GetStat();
            Console.WriteLine("entries: " + stat.ms_entries);
            Console.WriteLine("MaxKeySize: " + env.MaxKeySize);
            Console.WriteLine("ReaderCheck: " + env.ReaderCheck());
            env.Close();
        }

        [Test, Explicit("")]
        public void CouldTouchSpace()
        {
            // Assert.AreEqual(LMDBVersionInfo.Version, "LMDB 0.9.22: (March 21, 2018)");
            Console.WriteLine(LMDBVersionInfo.Version);
            var env = LMDBEnvironment.Create(TestUtils.GetPath());
            env.MapSize = 10 * 1024 * 1024;
            env.Open();
            var used = env.TouchSpace(5);
            var stat = env.GetStat();
            Console.WriteLine("Used size: " + used);
            env.Close();

            Console.WriteLine("Touch default: ");

            Console.WriteLine(LMDBVersionInfo.Version);
            env = LMDBEnvironment.Create(TestUtils.GetPath());
            env.MapSize = 10 * 1024 * 1024;
            env.Open();
            used = env.TouchSpace();
            stat = env.GetStat();
            Console.WriteLine("Used size: " + used);
            env.Close();
        }

        [Test, Explicit("")]
        public void CouldCreateManyEnvironment()
        {
            var path = TestUtils.GetPath();
            Console.WriteLine(LMDBVersionInfo.Version);
            for (int i = 0; i < 10; i++)
            {
                var env = LMDBEnvironment.Create(path + i);
                env.MapSize = 10L * 1024 * 1024 * 1024;
                env.Open();
                var stat = env.GetStat();
            }
        }

        // TODO fix
        [Test, Explicit("hangs")]
        public async Task CouldWriteAsync()
        {
            var path = TestUtils.GetPath();
            var env = LMDBEnvironment.Create(path, disableAsync:false);
            env.Open();
            var stat = env.GetStat();

            using (var db = env.OpenDatabase("first_db", new DatabaseConfig(DbFlags.Create)))
            {
                db.Truncate();

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
                    txn.Commit();
                    return Task.CompletedTask;
                }, false).ConfigureAwait(false);
            }

            env.Close();
        }

        [Test]
        public unsafe void CouldReserve()
        {
            var path = TestUtils.GetPath();
            var env = LMDBEnvironment.Create(path,
                LMDBEnvironmentFlags.WriteMap | LMDBEnvironmentFlags.NoSync);
            env.MapSize = 126 * 1024 * 1024;

            env.Open();

            var db = env.OpenDatabase("db_reserve", new DatabaseConfig(DbFlags.Create));

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
                            var key = new DirectBuffer(keyBytes.Length, (nint)keyPtr);
                            var value = new DirectBuffer(valueBytes.Length, (nint)valPtr);

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
                        var key = new DirectBuffer(keyBytes.Length, (nint)keyPtr);
                        var value = new DirectBuffer(valueBytes.Length, (nint)valPtr);

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
            db.Dispose();
            env.Close();
        }

        [Test]
        public unsafe void CouldReserve2()
        {
            var path = TestUtils.GetPath();
            var env = LMDBEnvironment.Create(path, LMDBEnvironmentFlags.WriteMap);
            env.MapSize = 16 * 1024 * 1024;

            env.Open();

            var db = env.OpenDatabase("db_reserve", new DatabaseConfig(DbFlags.Create | DbFlags.IntegerKey));

            DirectBuffer SharedBuffer;
            var _ = env.PageSize - env.OverflowPageHeaderSize;
            using (var txn = env.BeginTransaction())
            {
                try
                {
                    long sharedPid = 0;
                    var keyPtr = Unsafe.AsPointer(ref sharedPid);
                    var key1 = new DirectBuffer(8, (nint)keyPtr);

                    if (db.TryGet(txn, ref key1, out DirectBuffer value1))
                    {
                        SharedBuffer = value1;
                    }
                    else
                    {
                        var value = new DirectBuffer(env.PageSize - env.OverflowPageHeaderSize, 1);
                        // Note: DirectBuffer used to have an unsafe ctor that accepts null for data,
                        // here we emulate this behavior (the layout is fixed and won't change because it matches MDB_VAL):
                        Unsafe.AddByteOffset(ref Unsafe.As<DirectBuffer, nint>(ref Unsafe.AsRef(in value)), (nuint)IntPtr.Size) = IntPtr.Zero;
                        
                        db.Put(txn, ref key1, ref value, TransactionPutOptions.ReserveSpace);
                        value.Clear(0, value.Length);
                        SharedBuffer = value;
                    }

                    txn.Commit();
                }
                catch (Exception ex)
                {
                    txn.Abort();
                    Trace.TraceError(ex.ToString());
                    throw;
                }
            }

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
                            var key = new DirectBuffer(keyBytes.Length, (nint)keyPtr);
                            var value = new DirectBuffer(valueBytes.Length, (nint)valPtr);

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
                        var key = new DirectBuffer(keyBytes.Length, (nint)keyPtr);
                        var value = new DirectBuffer(valueBytes.Length, (nint)valPtr);

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
            db.Dispose();
            env.Close();
        }

        [Test]
        public void CouldWrite()
        {
            var path = TestUtils.GetPath();
            var env = LMDBEnvironment.Create(path,
                LMDBEnvironmentFlags.WriteMap | LMDBEnvironmentFlags.NoSync);
            env.Open();
            var stat = env.GetStat();

            var db = env.OpenDatabase("first_db", new DatabaseConfig(DbFlags.Create));

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

                txn.Commit();
            });
            db.Dispose();
            env.Close();
        }

        [Test]
        public unsafe void CouldWriteString()
        {
            var path = TestUtils.GetPath();
            var env = LMDBEnvironment.Create(path,
                LMDBEnvironmentFlags.WriteMap | LMDBEnvironmentFlags.NoSync);
            env.Open();
            var stat = env.GetStat();

            var db = env.OpenDatabase("first_db", new DatabaseConfig(DbFlags.Create));

            var keyString = "my_string_key";
            var values = new byte[] { 1, 2, 3, 4 };

            env.Write(txn =>
            {
                fixed (char* keyPtr = keyString)
                {
                    var keyUtf8Length = Encoding.UTF8.GetByteCount(keyString);
                    var keyBytes = stackalloc byte[keyUtf8Length];
                    Encoding.UTF8.GetBytes(keyPtr, keyString.Length, keyBytes, keyUtf8Length);
                    var key = new DirectBuffer(keyUtf8Length, (nint)keyBytes);

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

                    txn.Commit();
                }
            });
            db.Dispose();
            env.Close();
        }

        [Test, Explicit("long runnning")]
        public void CouldOpenHugeEnv()
        {
            var env = LMDBEnvironment.Create("C:/localdata/tmp/TestData/HugeEnv", LMDBEnvironmentFlags.WriteMap | LMDBEnvironmentFlags.NoSync);
            env.MapSize = 2 * 1024L * 1024 * 1024 * 1024L;
            env.Open();

            var db = env.OpenDatabase("db_reserve", new DatabaseConfig(DbFlags.Create));
            var keyBytes = new byte[] { 1, 2, 3, 4 };

            {
                env.Write(txn =>
                {
                    // var db2 = new Database("db_reserve", txn._impl, new DatabaseConfig(DbFlags.Create));

                    //var addr = 0L;
                    //var c = db.OpenCursor(txn);
                    //for (int i = 0; i < 50; i++)
                    //{
                    //    keyBytes[0] = (byte)i;

                    //    fixed (byte* keyPtr = &keyBytes[0])
                    //    {
                    //        var key = new DirectBuffer(4, keyPtr);
                    //        var value = new DirectBuffer(2000_000_000, keyPtr);
                    //        db.Put(txn, ref key, ref value, TransactionPutOptions.ReserveSpace);
                    //    }
                    //}
                    //c.Dispose();
                    txn.Commit();
                }, false);
            }
            db.Dispose();
            env.Close();
        }

        [Test, Explicit("long runnning")]
        public async Task CouldWriteAndReadProfileReadPath()
        {
            var path = TestUtils.GetPath();
            var env = LMDBEnvironment.Create(path, disableAsync:false);
            env.Open();

            var db = env.OpenDatabase("first_db", new DatabaseConfig(DbFlags.Create));

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
            }, false).ConfigureAwait(false);

            env.Read(txn =>
            {
                var key = new DirectBuffer(values);
                var value = new DirectBuffer(values);
                DirectBuffer value2 = default;

                var count = 1_000_000;

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
            db.Dispose();
            env.Close();
        }

        [Test, Explicit("long runnning")]
        public async Task CouldWriteAndReadProfileWriteAsyncPath()
        {
            var path = TestUtils.GetPath();
            var env = LMDBEnvironment.Create(path,
                // for any other config we have SQLite :)
                LMDBEnvironmentFlags.WriteMap | LMDBEnvironmentFlags.NoSync, disableAsync:false);
            env.Open();

            var db = env.OpenDatabase("first_db", new DatabaseConfig(DbFlags.Create));

            var values = new byte[] { 1, 2, 3, 4 };

            var count = 1_000;

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
                    }, false).ConfigureAwait(false);
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
            db.Dispose();
            env.Close();
        }

        [Test, Explicit("long runnning")]
        public void CouldWriteAndReadProfileWriteSYNCPath()
        {
            var path = TestUtils.GetPath();
            var env = LMDBEnvironment.Create(path, LMDBEnvironmentFlags.WriteMap | LMDBEnvironmentFlags.NoSync);
            env.Open();

            var db = env.OpenDatabase("first_db", new DatabaseConfig(DbFlags.Create));

            var values = new byte[] { 1, 2, 3, 4 };

            var count = 1_000;

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
            db.Dispose();
            env.Close();
        }

        [Test, Explicit("long runnning")]
        public async Task CouldWriteDupfixed()
        {
            var path = TestUtils.GetPath();
            var env = LMDBEnvironment.Create(path, LMDBEnvironmentFlags.WriteMap | LMDBEnvironmentFlags.NoSync);

            env.MapSize = 100 * 1024 * 1024;
            env.Open();

            var db = env.OpenDatabase("dupfixed_db",
                new DatabaseConfig(DbFlags.Create | DbFlags.IntegerDuplicates));

            //await db.Drop();
            //db = await env.OpenDatabase("dupfixed_db",
            //    new DatabaseConfig(DbFlags.Create | DbFlags.IntegerDuplicates));

            var valueHolder = new int[1];
            var mem = new Memory<int>(valueHolder);
            var handle = mem.Pin();
            var count = 1_000;

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
                }
            }

            handle.Dispose();

            Benchmark.Dump();
            db.Dispose();
            env.Close();
        }

        [Test]
        public void CouldWriteLongDups()
        {
            var path = TestUtils.GetPath();
            var env = LMDBEnvironment.Create(path, LMDBEnvironmentFlags.WriteMap | LMDBEnvironmentFlags.NoSync);

            env.MapSize = 100 * 1024 * 1024;
            env.Open();

            var db = env.OpenDatabase("dupfixed_db",
                new DatabaseConfig(DbFlags.Create | DbFlags.IntegerDuplicates));
            ulong count = 100;

            using (Benchmark.Run("Long Wrt+TFD", (long)count))
            {
                for (ulong i = 1; i < count; i++)
                {
                    var key = 0;
                    var value = i;
                    try
                    {
                        db.Put(0, i, TransactionPutOptions.AppendDuplicateData);

                        using (var txn = env.BeginReadOnlyTransaction())
                        {
                            if (!db.TryFindDup(txn, Lookup.EQ, ref key, ref value))
                            {
                                Assert.Fail("!db.TryGet(txn, ref key, out value)");
                            }

                            if (value != i)
                            {
                                Assert.Fail($"value {value} != i {i}");
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.ToString());
                    }
                }
            }

            Benchmark.Dump();
            db.Dispose();
            env.Dispose();
        }

        [Test, Explicit("Must support value fixed-size tuple, Spreads.Core serializes them as fixed size.")]
        public void CouldWriteTupleDups()
        {
            var path = TestUtils.GetPath();
            var env = LMDBEnvironment.Create(path, LMDBEnvironmentFlags.WriteMap | LMDBEnvironmentFlags.NoSync);

            env.MapSize = 100 * 1024 * 1024;
            env.Open();

            var db = env.OpenDatabase("dupfixed_db",
                new DatabaseConfig(DbFlags.Create | DbFlags.IntegerDuplicates));
            ulong count = 100;

            using (Benchmark.Run("Long Wrt+TFD", (long)count))
            {
                for (ulong i = 1; i < count; i++)
                {
                    var key = 0;
                    var value = (i, i);
                    try
                    {
                        db.Put(0, (i, i), TransactionPutOptions.AppendDuplicateData);

                        using (var txn = env.BeginReadOnlyTransaction())
                        {
                            if (!db.TryFindDup(txn, Lookup.EQ, ref key, ref value))
                            {
                                Assert.Fail("!db.TryGet(txn, ref key, out value)");
                            }

                            if (value != (i, i))
                            {
                                Assert.Fail($"value {value} != (i,i) {(i, i)}");
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.ToString());
                    }
                }
            }

            Benchmark.Dump();
            db.Dispose();
            env.Dispose();
        }

        [Test]
        public async Task CouldDeleteDupSorted()
        {
            var path = TestUtils.GetPath();
            var env = LMDBEnvironment.Create(path, LMDBEnvironmentFlags.WriteMap | LMDBEnvironmentFlags.NoSync);

            env.MapSize = 100 * 1024 * 1024;
            env.Open();

            var db = env.OpenDatabase("dupfixed_db",
                new DatabaseConfig(DbFlags.Create | DbFlags.IntegerDuplicates));
            db.Truncate();

            var count = 10_000;

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
                foreach (var kvp in db.AsEnumerable<int, int>(txn))
                {
                    Console.WriteLine($"kvp: {kvp.Key} - {kvp.Value}");
                }

                Assert.AreEqual(count, db.AsEnumerable<int, int>(txn, 0).Count());
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

                Assert.AreEqual(count - 1, db.AsEnumerable<int, int>(txn, 0).Count());
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
            db.Dispose();
            env.Close();
        }

        [Test, Explicit("long runnning")]
        public async Task CouldWriteDupfixedFromTwoThreads()
        {
            var path = TestUtils.GetPath();
            var env = LMDBEnvironment.Create(path, LMDBEnvironmentFlags.WriteMap | LMDBEnvironmentFlags.NoSync);

            env.MapSize = 100 * 1024 * 1024;
            env.Open();

            var key = 0L;

            var count = 1_000;

            var db = env.OpenDatabase("dupfixed_db",
                new DatabaseConfig(DbFlags.Create | DbFlags.IntegerDuplicates));

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
            db.Dispose();
            env.Close();
        }

        [StructLayout(LayoutKind.Sequential, Pack = 8, Size = 16)]
        public struct InPlaceUpdateable
        {
            public long Key;
            public long Value;
        }

        [Test, Explicit("long runnning")]
        public async Task CouldUpdateInplaceFromAbortedWriteTransactions()
        {
            var path = TestUtils.GetPath();
            var env = LMDBEnvironment.Create(path, LMDBEnvironmentFlags.WriteMap | LMDBEnvironmentFlags.NoSync);

            env.MapSize = 100 * 1024 * 1024;
            env.Open();

            var items = 1_000;
            var count = items * 100;
            var counts = new long[count];
            var changedPointers = new IntPtr[count];
            var finalPointers = new IntPtr[count];

            // one thread fills DB with new values, while another increments in-place values while it can and total
            // sum of

            var db = env.OpenDatabase("update_inplace",
                new DatabaseConfig(DbFlags.Create | DbFlags.IntegerKey));
            db.Truncate();

            var t1 = Task.Run(() =>
            {
                using (Benchmark.Run("Writer (Kops)", items * 1000))
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

                                    changedPointers[value.ReadInt64(0)] = DbSafePtr(value);
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
                                    changedPointers[value.ReadInt64(0)] = DbSafePtr(value);
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
                                    changedPointers[value.ReadInt64(0)] = DbSafePtr(value);
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
                                    changedPointers[value.ReadInt64(0)] = DbSafePtr(value);
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
                    finalPointers[value.ReadInt64(0)] = DbSafePtr(value);
                    cnt++;
                    while (c.TryGet(ref key, ref value, CursorGetOption.Next))
                    {
                        sum += value.ReadInt64(8);
                        finalPointers[value.ReadInt64(0)] = DbSafePtr(value);
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
            db.Dispose();
            env.Close();
        }

        [Test]
        public void CouldOpenMultipleEnvironmentsInTheSameProcess()
        {
            var path = TestUtils.GetPath();
            var env1 = LMDBEnvironment.Create(path);
            env1.Open();
            var env2 = LMDBEnvironment.Create(path);
            env2.Open();
            Assert.IsTrue(ReferenceEquals(env1, env2));
            Assert.IsTrue(env2.IsOpen);
            env2.Close();
            Assert.IsTrue(env1.IsOpen);
            var env3 = LMDBEnvironment.Create(path);
            Assert.IsTrue(ReferenceEquals(env1, env3));
            env1.Close();
            Assert.IsTrue(env3.IsOpen);
            env3.Close();
            Assert.IsFalse(env3.IsOpen);
        }

        [Test]
        public void CouldGetOverflowPageHeaderLength()
        {
            var path = TestUtils.GetPath();
            var env = LMDBEnvironment.Create(path, LMDBEnvironmentFlags.WriteMap);
            env.Open();
            Console.WriteLine("Page size: " + env.PageSize);
            Assert.AreEqual(16, env.OverflowPageHeaderSize);
            Console.WriteLine("Overflow header size: " + env.OverflowPageHeaderSize);
            var stat = env.GetStat();
            var info = env.GetEnvInfo();
            Console.WriteLine("OFP: " + stat.ms_overflow_pages);
        }

        private static unsafe IntPtr DbSafePtr(DirectBuffer db)
        {
            return (IntPtr)db.Data;
        }

        [Test]
        public void CouldOpenRoCursorFromWriteTxn()
        {
            var path = TestUtils.GetPath();
            var env = LMDBEnvironment.Create(path, LMDBEnvironmentFlags.WriteMap);
            env.Open();

            var db = env.OpenDatabase("first_db", new DatabaseConfig(DbFlags.Create));

            using (var txn = env.BeginTransaction())
            using (var cursor = db.OpenReadOnlyCursor((ReadOnlyTransaction)txn))
            using (var cursor2 = db.OpenReadOnlyCursor(txn))
            {
            }
            db.Dispose();
            env.Close();
        }


        [Test]
        public unsafe void CursorCanTryGetWhenAllRecordsDeleted()
        {
            var path = TestUtils.GetPath();
            var env = LMDBEnvironment.Create(path, LMDBEnvironmentFlags.WriteMap);
            env.Open();
            var key = 42L;
            // TODO this is not pinned and only works by luck, until GC moves the byte[] buffer
            var kdb = new DirectBuffer(BitConverter.GetBytes(key));
            const int count = 20;
            var r = new Random();
            using (var db = env.OpenDatabase("first_db", new DatabaseConfig(DbFlags.Create | DbFlags.DuplicatesSort) {DupSortPrefix = 64}))
            {
                db.Truncate();
                using (var tx = env.BeginTransaction())
                {
                    var buffer = new byte[64];
                    for (int i = 0; i < count; i++)
                    {
                        long v = i+1;
                        r.NextBytes(buffer);
                        var bytes = BitConverter.GetBytes(v).Concat(buffer).ToArray();
                        var valdb = new DirectBuffer(bytes);
                        db.Put(tx, ref kdb, ref valdb, TransactionPutOptions.AppendDuplicateData);
                    }

                    tx.Commit();
                }

                using (var tx = env.BeginTransaction())
                {
                    using (var c = db.OpenCursor(tx))
                    {
                        long prefix = 1L;
                        Assert.IsTrue(c.TryFindDup(Lookup.EQ, ref key, ref prefix));
                        Assert.AreEqual(1, prefix);

                        // c is now at dup 1
                        while(c.Delete(false))
                        {
                            // One way to detect when all values are deleted is to call the method with CursorGetOption.Set option.
                            // We need to check is kdb exists, and it is deleted when the last dupsorted value is deleted.
                            //if (!c.TryGet(ref key, ref prefix, CursorGetOption.Set))
                            //{
                            //    break;
                            //}

                            // We cannot call GetCurrent after deleting the last dupsorted value,
                            // because GetCurrent does not move the cursor. It is the only cursor
                            // operation (other then multi ops which are not supported by this lib so far)
                            // that does not move the cursors, but depends on previous moves.
                            // http://www.lmdb.tech/doc/group__mdb.html#ga1206b2af8b95e7f6b0ef6b28708c9127
                            // If you want to move cursor to both key and dubsorted value then use
                            // CursorGetOption.GetBoth for exact match, or CursorGetOption.GetBothRange, which
                            // "positions at key, nearest data.", but does not specify nearest to which direction.
                            // It's better to call Spreads's extension TryFindDup with Lookup option,
                            // it behaves much more intuitively and does all required work on C side, saving P/Invoke calls.
                            if (c.TryGet(ref key, ref prefix, CursorGetOption.GetBothRange))
                            {
                                Console.WriteLine(prefix);
                            }
                            else
                            {
                                // need to exit the loop if there are no more values, delete works on current value
                                // which is invalid after deleting the last one.
                                break;
                            }
                        }
                    }
                }
            }

            env.Dispose();
        }

        [Test]
        public unsafe void Issue24()
        {
            var path = TestUtils.GetPath();
            var env = LMDBEnvironment.Create(path, LMDBEnvironmentFlags.WriteMap);
            env.Open();
            var key = "salamo";
            var value = "simoliakho";

            var keyLen = Encoding.UTF8.GetByteCount(key);
            var keyBytes = stackalloc byte[keyLen];

            var valLen = Encoding.UTF8.GetByteCount(value);
            var valBytes = stackalloc byte[valLen];

            fixed (char* keyCPtr = key, valCPtr = value)
            {
                Encoding.UTF8.GetBytes(keyCPtr, key.Length, keyBytes, keyLen);
                Encoding.UTF8.GetBytes(valCPtr, value.Length, valBytes, valLen);
            }

            using (var db = env.OpenDatabase("first_db", new DatabaseConfig(DbFlags.Create)))
            {
                using (var tx = env.BeginTransaction())
                {
                    var keydb = new DirectBuffer(keyLen, (nint)keyBytes);
                    var valuedb = new DirectBuffer(valLen, (nint)valBytes);
                    db.Put(tx, ref keydb, ref valuedb, TransactionPutOptions.NoDuplicateData);

                    tx.Commit();
                }

                using (var tx = env.BeginReadOnlyTransaction())
                {
                    var keydb = new DirectBuffer(keyLen, (nint)keyBytes);
                    DirectBuffer valuedb = default;
                    Assert.IsTrue(db.TryGet(tx, ref keydb, out valuedb));
                    Assert.AreEqual(value, Encoding.UTF8.GetString(valuedb.Span.ToArray()));
                }

                using (var tx = env.BeginReadOnlyTransaction())
                using (var c = db.OpenReadOnlyCursor(tx))
                {
                    var keydb = new DirectBuffer(keyLen, (nint)keyBytes);
                    DirectBuffer valuedb = default;
                    Assert.IsTrue(c.TryFind(Spreads.Lookup.EQ, ref keydb, out valuedb));
                }
            }

            env.Dispose();
        }
    }
}
