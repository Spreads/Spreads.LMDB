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
            Assert.AreEqual(LMDBVersionInfo.Version, "LMDB 0.9.22: (March 21, 2018)");
            Console.WriteLine(LMDBVersionInfo.Version);
            var env = new Environment("./Data");
            env.Open();
            var stat = env.GetStat();
            Console.WriteLine("entries: " + stat.ms_entries);
            Console.WriteLine("MaxKeySize: " + env.MaxKeySize);
            await env.Close();
        }

        [Test]
        public async Task CouldWriteAndRead()
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

                var count = 20_000_000;

                using (Benchmark.Run("Read path via cursor recreation", count))
                {
                    for (int i = 0; i < count; i++)
                    {
                        using (var cursor = db.OpenReadOnlyCursor(txn))
                        {
                            cursor.TryGet(CursorGetOption.SetKey, ref key, ref value2);
                        }
                    }
                }

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
    }
}