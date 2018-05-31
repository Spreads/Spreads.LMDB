// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

using NUnit.Framework;
using Spreads.LMDB.Interop;
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
            await env.Close();
        }

        [Test]
        public async Task CouldWriteAndRead()
        {
            Assert.AreEqual(LMDBVersionInfo.Version, "LMDB 0.9.22: (March 21, 2018)");
            Console.WriteLine(LMDBVersionInfo.Version);
            var env = new Environment("./Data");
            env.Open();
            var stat = env.GetStat();

            var db = await env.OpenDatabase("first_db", new DatabaseConfig(DbFlags.Create));

            await env.WriteAsync(txn =>
            {
                var cursor = db.OpenWriteCursor(txn);
                var values = new byte[] { 1, 2, 3, 4 };
                var key = new MDB_val(values);
                var value = new MDB_val(values);
                MDB_val value2 = default;

                Assert.IsTrue(cursor.TryPut(ref key, ref value, CursorPutOptions.NoOverwrite));

                Assert.IsTrue(cursor.TryGet(CursorGetOption.SetKey, ref key, ref value2));

                Assert.IsTrue(value2.Span.SequenceEqual(value.Span));

                return Task.CompletedTask;
            });

            await env.Close();
        }
    }
}