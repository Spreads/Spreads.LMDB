// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

using NUnit.Framework;
using Spreads.LMDB.Interop;
using Spreads.Utils;
using System;
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
            var env = new Environment("./Data/test_db");
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
                var env = new Environment("./Data/test_db" + i);
                env.MapSize = 10L * 1024 * 1024 * 1024;
                env.Open();
                var stat = env.GetStat();
            }

            await Task.Delay(60000000);
        }

        [Test]
        public async Task CouldWriteAsync()
        {
            var env = new Environment("./Data");
            env.Open();
            var stat = env.GetStat();

            var db = await env.OpenDatabase("first_db", new DatabaseConfig(DbFlags.Create));

            var values = new byte[] { 1, 2, 3, 4 };

            await env.WriteAsync(txn =>
            {
                var key = new MDB_val(values);
                var value = new MDB_val(values);
                MDB_val value2 = default;

                using (var cursor = db.OpenCursor(txn))
                {
                    Assert.IsTrue(cursor.TryPut(ref key, ref value, CursorPutOptions.NoOverwrite));
                }

                using (var cursor = db.OpenCursor(txn))
                {
                    Assert.IsTrue(cursor.TryGet(CursorGetOption.SetKey, ref key, ref value2));
                }

                Assert.IsTrue(value2.Span.SequenceEqual(value.Span));

                return Task.CompletedTask;
            });

            await env.Close();
        }

        [Test]
        public void CouldWrite()
        {
            var env = new Environment("./Data");
            env.Open();
            var stat = env.GetStat();

            var db = env.OpenDatabase("first_db", new DatabaseConfig(DbFlags.Create)).Result;

            var values = new byte[] { 1, 2, 3, 4 };

            env.Write(txn =>
            {
                var key = new MDB_val(values);
                var value = new MDB_val(values);
                MDB_val value2 = default;

                using (var cursor = db.OpenCursor(txn))
                {
                    Assert.IsTrue(cursor.TryPut(ref key, ref value, CursorPutOptions.None));
                }

                using (var cursor = db.OpenCursor(txn))
                {
                    Assert.IsTrue(cursor.TryGet(CursorGetOption.SetKey, ref key, ref value2));
                }

                Assert.IsTrue(value2.Span.SequenceEqual(value.Span));

                ;
            });

            env.Close().Wait();
        }

        [Test, Explicit("long runnning")]
        public async Task CouldWriteAndReadProfileReadPath()
        {
            var env = new Environment("./Data");
            env.Open();

            var db = await env.OpenDatabase("first_db", new DatabaseConfig(DbFlags.Create));

            var values = new byte[] { 1, 2, 3, 4 };

            await env.WriteAsync(txn =>
            {
                var key = new MDB_val(values);
                var value = new MDB_val(values);
                MDB_val value2 = default;

                using (var cursor = db.OpenCursor(txn))
                {
                    Assert.IsTrue(cursor.TryPut(ref key, ref value, CursorPutOptions.None));
                }
                txn.Commit();
                return null;
            });

            env.Read(txn =>
            {
                var key = new MDB_val(values);
                var value = new MDB_val(values);
                MDB_val value2 = default;

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
                            cursor.TryGet(CursorGetOption.SetKey, ref key, ref value2);
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
            var env = new Environment("./Data",
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
                        var key = new MDB_val(values);
                        var value = new MDB_val(values);
                        MDB_val value2 = default;

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
                var key = new MDB_val(values);
                var value = new MDB_val(values);
                MDB_val value2 = default;

                using (var cursor = db.OpenReadOnlyCursor(txn))
                {
                    Assert.IsTrue(cursor.TryGet(CursorGetOption.SetKey, ref key, ref value2));
                }
                Assert.IsTrue(value2.Span.SequenceEqual(value.Span));

                return true;
            });

            await env.Close();
        }

        [Test, Explicit("long runnning")]
        public void CouldWriteAndReadProfileWriteSYNCPath()
        {
            var env = new Environment("./Data",
                // for any other config we have SQLite :)
                DbEnvironmentFlags.WriteMap | DbEnvironmentFlags.NoSync);
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
                        var key = new MDB_val(values);
                        var value = new MDB_val(values);
                        MDB_val value2 = default;

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
                var key = new MDB_val(values);
                var value = new MDB_val(values);
                MDB_val value2 = default;

                using (var cursor = db.OpenReadOnlyCursor(txn))
                {
                    Assert.IsTrue(cursor.TryGet(CursorGetOption.SetKey, ref key, ref value2));
                }
                Assert.IsTrue(value2.Span.SequenceEqual(value.Span));

                return true;
            });

            env.Close().Wait();
        }
    }
}