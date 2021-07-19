// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

using NUnit.Framework;
using Spreads.Utils;
using System;
using System.IO;
using System.Runtime.InteropServices;

namespace Spreads.LMDB.Tests
{
    [TestFixture]
    public class SpreadsMethodsTests
    {
        [Test]
        public void CouldFindNoDup()
        {
            var env = LMDBEnvironment.Create("./Data/CouldFindNoDup",
                LMDBEnvironmentFlags.WriteMap | LMDBEnvironmentFlags.NoSync);
            env.Open();

            var db = env.OpenDatabase("CouldFindNoDup", new DatabaseConfig(DbFlags.Create | DbFlags.IntegerKey));
            db.Truncate();

            var txn = env.BeginTransaction();
            try
            {
                var midKey = 100;
                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsFalse(c.TryFind(Lookup.LT, ref midKey, out int _));
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsFalse(c.TryFind(Lookup.LE, ref midKey, out int _));
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsFalse(c.TryFind(Lookup.EQ, ref midKey, out int _));
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsFalse(c.TryFind(Lookup.GE, ref midKey, out int _));
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsFalse(c.TryFind(Lookup.GT, ref midKey, out int _));
                }

                /////////////////////////////////////////////////////////////////
                Assert.AreEqual(100, midKey);
                db.Put(txn, midKey, 1);

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsFalse(c.TryFind(Lookup.LT, ref midKey, out int _));
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFind(Lookup.LE, ref midKey, out int _));
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFind(Lookup.EQ, ref midKey, out int _));
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFind(Lookup.GE, ref midKey, out int _));
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsFalse(c.TryFind(Lookup.GT, ref midKey, out int _));
                }

                /////////////////////////////////////////////////////////////////
                Assert.AreEqual(100, midKey);
                db.Put(txn, midKey + 10, 1);

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsFalse(c.TryFind(Lookup.LT, ref midKey, out int _));
                }
                Assert.AreEqual(100, midKey);

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFind(Lookup.LE, ref midKey, out int _));
                }
                Assert.AreEqual(100, midKey);

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFind(Lookup.EQ, ref midKey, out int _));
                }
                Assert.AreEqual(100, midKey);

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFind(Lookup.GE, ref midKey, out int _));
                }
                Assert.AreEqual(100, midKey);

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFind(Lookup.GT, ref midKey, out int _));
                }
                Assert.AreEqual(110, midKey);

                /////////////////////////////////////////////////////////////////
                midKey = 100;
                db.Put(txn, midKey - 10, 1);

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFind(Lookup.LT, ref midKey, out int _));
                }
                Assert.AreEqual(90, midKey);
                midKey = 100;

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFind(Lookup.LE, ref midKey, out int _));
                }
                Assert.AreEqual(100, midKey);

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFind(Lookup.EQ, ref midKey, out int _));
                }
                Assert.AreEqual(100, midKey);

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFind(Lookup.GE, ref midKey, out int _));
                }
                Assert.AreEqual(100, midKey);

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFind(Lookup.GT, ref midKey, out int _));
                }
                Assert.AreEqual(110, midKey);
                midKey = 100;

                /////////////////////////////////////////////////////////////////
                var bigKey = midKey + 100;

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFind(Lookup.LT, ref bigKey, out int _));
                    Assert.AreEqual(110, bigKey);
                    bigKey = midKey + 100;
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFind(Lookup.LE, ref bigKey, out int _));
                    Assert.AreEqual(110, bigKey);
                    bigKey = midKey + 100;
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsFalse(c.TryFind(Lookup.EQ, ref bigKey, out int _));
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsFalse(c.TryFind(Lookup.GE, ref bigKey, out int _));
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsFalse(c.TryFind(Lookup.GT, ref bigKey, out int _));
                }

                /////////////////////////////////////////////////////////////////
                var smallKey = midKey - 50;

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsFalse(c.TryFind(Lookup.LT, ref smallKey, out int _));
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsFalse(c.TryFind(Lookup.LE, ref smallKey, out int val));
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsFalse(c.TryFind(Lookup.EQ, ref smallKey, out int _));
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFind(Lookup.GE, ref smallKey, out int _));
                    Assert.AreEqual(90, smallKey);
                    smallKey = midKey - 50;
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFind(Lookup.GT, ref smallKey, out int _));
                    Assert.AreEqual(90, smallKey);
                    smallKey = midKey - 50;
                }

                /////////////////////////////////////////////////////////////////
                var midBigger = midKey + 5;

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFind(Lookup.LT, ref midBigger, out int _));
                    Assert.AreEqual(100, midBigger);
                    midBigger = midKey + 5;
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFind(Lookup.LE, ref midBigger, out int _));
                    Assert.AreEqual(100, midBigger);
                    midBigger = midKey + 5;
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsFalse(c.TryFind(Lookup.EQ, ref midBigger, out int _));
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFind(Lookup.GE, ref midBigger, out int _));
                    Assert.AreEqual(110, midBigger);
                    midBigger = midKey + 5;
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFind(Lookup.GT, ref midBigger, out int _));
                    Assert.AreEqual(110, midBigger);
                    midBigger = midKey + 5;
                }

                /////////////////////////////////////////////////////////////////
                var midSmaller = midKey - 5;

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFind(Lookup.LT, ref midSmaller, out int _));
                    Assert.AreEqual(90, midSmaller);
                    midSmaller = midKey - 5;
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFind(Lookup.LE, ref midSmaller, out int _));
                    Assert.AreEqual(90, midSmaller);
                    midSmaller = midKey - 5;
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsFalse(c.TryFind(Lookup.EQ, ref midSmaller, out int _));
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFind(Lookup.GE, ref midSmaller, out int _));
                    Assert.AreEqual(100, midSmaller);
                    midSmaller = midKey - 5;
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFind(Lookup.GT, ref midSmaller, out int _));
                    Assert.AreEqual(100, midSmaller);
                    midSmaller = midKey - 5;
                }

                /////////////////////////////////////////////////////////////////
                var biggerKey = midKey + 10;

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFind(Lookup.LT, ref biggerKey, out int _));
                    Assert.AreEqual(100, biggerKey);
                    biggerKey = midKey + 10;
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFind(Lookup.LE, ref biggerKey, out int _));
                    Assert.AreEqual(110, biggerKey);
                    biggerKey = midKey + 10;
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFind(Lookup.EQ, ref biggerKey, out int _));
                    Assert.AreEqual(110, biggerKey);
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFind(Lookup.GE, ref biggerKey, out int _));
                    Assert.AreEqual(110, biggerKey);
                    biggerKey = midKey + 10;
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsFalse(c.TryFind(Lookup.GT, ref biggerKey, out int _));
                    Assert.AreEqual(110, biggerKey);
                    biggerKey = midKey + 10;
                }

                /////////////////////////////////////////////////////////////////
                var smallerKey = midKey - 10;

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsFalse(c.TryFind(Lookup.LT, ref smallerKey, out int _));
                    Assert.AreEqual(90, smallerKey);
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFind(Lookup.LE, ref smallerKey, out int _));
                    Assert.AreEqual(90, smallerKey);
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFind(Lookup.EQ, ref smallerKey, out int _));
                    Assert.AreEqual(90, smallerKey);
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFind(Lookup.GE, ref smallerKey, out int _));
                    Assert.AreEqual(90, smallerKey);
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFind(Lookup.GT, ref smallerKey, out int _));
                    Assert.AreEqual(100, smallerKey);
                    smallerKey = midKey - 10;
                }
            }
            finally
            {
                txn.Abort();
            }

            db.Dispose();
            env.Close();
        }

        [Test]
        public void CouldFindDup()
        {
            var env = LMDBEnvironment.Create(TestUtils.GetPath(),
                LMDBEnvironmentFlags.WriteMap | LMDBEnvironmentFlags.NoSync);
            env.Open();

            var db = env.OpenDatabase("CouldFindDup", new DatabaseConfig(DbFlags.Create | DbFlags.IntegerKey | DbFlags.DuplicatesFixed | DbFlags.IntegerDuplicates));
            db.Truncate();

            var nodupKey = 0;

            var txn = env.BeginTransaction();
            try
            {
                var midKey = 100;
                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsFalse(c.TryFindDup(Lookup.LT, ref nodupKey, ref midKey));
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsFalse(c.TryFindDup(Lookup.LE, ref nodupKey, ref midKey));
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsFalse(c.TryFindDup(Lookup.EQ, ref nodupKey, ref midKey));
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsFalse(c.TryFindDup(Lookup.GE, ref nodupKey, ref midKey));
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsFalse(c.TryFindDup(Lookup.GT, ref nodupKey, ref midKey));
                }

                /////////////////////////////////////////////////////////////////
                Assert.AreEqual(100, midKey);
                db.Put(txn, nodupKey, midKey);

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsFalse(c.TryFindDup(Lookup.LT, ref nodupKey, ref midKey));
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFindDup(Lookup.LE, ref nodupKey, ref midKey));
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFindDup(Lookup.EQ, ref nodupKey, ref midKey));
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFindDup(Lookup.GE, ref nodupKey, ref midKey));
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsFalse(c.TryFindDup(Lookup.GT, ref nodupKey, ref midKey));
                }

                /////////////////////////////////////////////////////////////////
                Assert.AreEqual(100, midKey);
                db.Put(txn, nodupKey, midKey + 10);

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsFalse(c.TryFindDup(Lookup.LT, ref nodupKey, ref midKey));
                }
                Assert.AreEqual(100, midKey);

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFindDup(Lookup.LE, ref nodupKey, ref midKey));
                }
                Assert.AreEqual(100, midKey);

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFindDup(Lookup.EQ, ref nodupKey, ref midKey));
                }
                Assert.AreEqual(100, midKey);

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFindDup(Lookup.GE, ref nodupKey, ref midKey));
                }
                Assert.AreEqual(100, midKey);

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFindDup(Lookup.GT, ref nodupKey, ref midKey));
                }
                Assert.AreEqual(110, midKey);

                /////////////////////////////////////////////////////////////////
                midKey = 100;
                db.Put(txn, nodupKey, midKey - 10);

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFindDup(Lookup.LT, ref nodupKey, ref midKey));
                }
                Assert.AreEqual(90, midKey);
                midKey = 100;

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFindDup(Lookup.LE, ref nodupKey, ref midKey));
                }
                Assert.AreEqual(100, midKey);

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFindDup(Lookup.EQ, ref nodupKey, ref midKey));
                }
                Assert.AreEqual(100, midKey);

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFindDup(Lookup.GE, ref nodupKey, ref midKey));
                }
                Assert.AreEqual(100, midKey);

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFindDup(Lookup.GT, ref nodupKey, ref midKey));
                }
                Assert.AreEqual(110, midKey);
                midKey = 100;

                /////////////////////////////////////////////////////////////////
                var bigKey = midKey + 100;

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFindDup(Lookup.LT, ref nodupKey, ref bigKey));
                    Assert.AreEqual(110, bigKey);
                    bigKey = midKey + 100;
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFindDup(Lookup.LE, ref nodupKey, ref bigKey));
                    Assert.AreEqual(110, bigKey);
                    bigKey = midKey + 100;
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsFalse(c.TryFindDup(Lookup.EQ, ref nodupKey, ref bigKey));
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsFalse(c.TryFindDup(Lookup.GE, ref nodupKey, ref bigKey));
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsFalse(c.TryFindDup(Lookup.GT, ref nodupKey, ref bigKey));
                }

                /////////////////////////////////////////////////////////////////
                var smallKey = midKey - 50;

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsFalse(c.TryFindDup(Lookup.LT, ref nodupKey, ref smallKey));
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsFalse(c.TryFindDup(Lookup.LE, ref nodupKey, ref smallKey));
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsFalse(c.TryFindDup(Lookup.EQ, ref nodupKey, ref smallKey));
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFindDup(Lookup.GE, ref nodupKey, ref smallKey));
                    Assert.AreEqual(90, smallKey);
                    smallKey = midKey - 50;
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFindDup(Lookup.GT, ref nodupKey, ref smallKey));
                    Assert.AreEqual(90, smallKey);
                    smallKey = midKey - 50;
                }

                /////////////////////////////////////////////////////////////////
                var midBigger = midKey + 5;

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFindDup(Lookup.LT, ref nodupKey, ref midBigger));
                    Assert.AreEqual(100, midBigger);
                    midBigger = midKey + 5;
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFindDup(Lookup.LE, ref nodupKey, ref midBigger));
                    Assert.AreEqual(100, midBigger);
                    midBigger = midKey + 5;
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsFalse(c.TryFindDup(Lookup.EQ, ref nodupKey, ref midBigger));
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFindDup(Lookup.GE, ref nodupKey, ref midBigger));
                    Assert.AreEqual(110, midBigger);
                    midBigger = midKey + 5;
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFindDup(Lookup.GT, ref nodupKey, ref midBigger));
                    Assert.AreEqual(110, midBigger);
                    midBigger = midKey + 5;
                }

                /////////////////////////////////////////////////////////////////
                var midSmaller = midKey - 5;

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFindDup(Lookup.LT, ref nodupKey, ref midSmaller));
                    Assert.AreEqual(90, midSmaller);
                    midSmaller = midKey - 5;
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFindDup(Lookup.LE, ref nodupKey, ref midSmaller));
                    Assert.AreEqual(90, midSmaller);
                    midSmaller = midKey - 5;
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsFalse(c.TryFindDup(Lookup.EQ, ref nodupKey, ref midSmaller));
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFindDup(Lookup.GE, ref nodupKey, ref midSmaller));
                    Assert.AreEqual(100, midSmaller);
                    midSmaller = midKey - 5;
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFindDup(Lookup.GT, ref nodupKey, ref midSmaller));
                    Assert.AreEqual(100, midSmaller);
                    midSmaller = midKey - 5;
                }

                /////////////////////////////////////////////////////////////////
                var biggerKey = midKey + 10;

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFindDup(Lookup.LT, ref nodupKey, ref biggerKey));
                    Assert.AreEqual(100, biggerKey);
                    biggerKey = midKey + 10;
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFindDup(Lookup.LE, ref nodupKey, ref biggerKey));
                    Assert.AreEqual(110, biggerKey);
                    biggerKey = midKey + 10;
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFindDup(Lookup.EQ, ref nodupKey, ref biggerKey));
                    Assert.AreEqual(110, biggerKey);
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFindDup(Lookup.GE, ref nodupKey, ref biggerKey));
                    Assert.AreEqual(110, biggerKey);
                    biggerKey = midKey + 10;
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsFalse(c.TryFindDup(Lookup.GT, ref nodupKey, ref biggerKey));
                    Assert.AreEqual(110, biggerKey);
                    biggerKey = midKey + 10;
                }

                /////////////////////////////////////////////////////////////////
                var smallerKey = midKey - 10;

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsFalse(c.TryFindDup(Lookup.LT, ref nodupKey, ref smallerKey));
                    Assert.AreEqual(90, smallerKey);
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFindDup(Lookup.LE, ref nodupKey, ref smallerKey));
                    Assert.AreEqual(90, smallerKey);
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFindDup(Lookup.EQ, ref nodupKey, ref smallerKey));
                    Assert.AreEqual(90, smallerKey);
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFindDup(Lookup.GE, ref nodupKey, ref smallerKey));
                    Assert.AreEqual(90, smallerKey);
                }

                using (var c = db.OpenCursor(txn))
                {
                    Assert.IsTrue(c.TryFindDup(Lookup.GT, ref nodupKey, ref smallerKey));
                    Assert.AreEqual(100, smallerKey);
                    smallerKey = midKey - 10;
                }
            }
            finally
            {
                txn.Abort();
            }
            db.Dispose();
            env.Close();
        }

        [StructLayout(LayoutKind.Sequential, Pack = 8, Size = 24)]
        public struct DupValueWithWideKey
        {
            public ulong First;
            public ulong Second;
            public long Value;
        }

        [Test]
        public void CouldFindDupWideKeyBatch()
        {
            var path = TestUtils.GetPath();
            Console.WriteLine(Path.GetFullPath(path));
            var env = LMDBEnvironment.Create(path,
                 LMDBEnvironmentFlags.NoSync | LMDBEnvironmentFlags.WriteMap);
            env.MapSize = 100 * 1024 * 1024;
            env.Open();

            var db = env.OpenDatabase("CouldFindDupWideKeyBatch",
                new DatabaseConfig(DbFlags.Create | DbFlags.DuplicatesFixed)
                {
                    DupSortPrefix = 64 * 64
                });
            db.Truncate();

            var nodupKey = 0;

            var count = 500;

            try
            {
                for (int i = 1; i <= count; i++)
                {
                    var dupValue = new DupValueWithWideKey()
                    {
                        First = (ulong)i,
                        Second = (ulong)i * 10 + 5,
                        Value = i * 10 + 5
                    };
                    db.Put(nodupKey, dupValue, TransactionPutOptions.AppendDuplicateData);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }


            using (Benchmark.Run("WideKeyRead", count * 5))
            using (var txn = env.BeginReadOnlyTransaction())
            using (var c = db.OpenReadOnlyCursor(txn))
            {
                var searchValue = new DupValueWithWideKey()
                {
                    First = 0,
                    Second = (ulong)count * 10 + 5,
                    Value = count * 10 + 5
                };

                Assert.IsFalse(c.TryFindDup(Lookup.GT, ref nodupKey, ref searchValue));
            }

            using (Benchmark.Run("WideKeyRead", count * 5))
            using (var txn = env.BeginReadOnlyTransaction())
            using (var c = db.OpenReadOnlyCursor(txn))
            {
                for (int i = 1; i <= count; i++)
                {
                    var searchValue = new DupValueWithWideKey()
                    {
                        First = 0,
                        Second = (ulong)i * 10, // without + 5, always smaller
                        Value = i * 10 + 5
                    };

                    Assert.IsTrue(c.TryFindDup(Lookup.GT, ref nodupKey, ref searchValue));

                    if (searchValue.Value != i * 10 + 5)
                    {
                        Console.WriteLine($"2: {searchValue.Value} - {i * 10 + 5}");
                        Assert.Fail("Wide key doesn't work");
                    }
                }

                for (int i = 1; i <= count; i++)
                {
                    var searchValue = new DupValueWithWideKey()
                    {
                        First = 0,
                        Second = (ulong)i * 10 + 5, // equal
                        Value = i * 10 + 5
                    };

                    c.TryFindDup(Lookup.EQ, ref nodupKey, ref searchValue);

                    if (searchValue.Value != i * 10 + 5)
                    {
                        Console.WriteLine($"2: {searchValue.Value} - {i * 10 + 5}");
                        Assert.Fail("Wide key doesn't work");
                    }

                    searchValue = new DupValueWithWideKey()
                    {
                        First = 0,
                        Second = (ulong)i * 10 + 5, // equal
                        Value = i * 10 + 5
                    };

                    c.TryFindDup(Lookup.GE, ref nodupKey, ref searchValue);

                    if (searchValue.Value != i * 10 + 5)
                    {
                        Console.WriteLine($"2: {searchValue.Value} - {i * 10 + 5}");
                        Assert.Fail("Wide key doesn't work");
                    }

                    searchValue = new DupValueWithWideKey()
                    {
                        First = 0,
                        Second = (ulong)i * 10 + 5, // equal
                        Value = i * 10 + 5
                    };

                    if (!c.TryFindDup(Lookup.LE, ref nodupKey, ref searchValue))
                    {
                        Assert.Fail("Cannot find");
                    }

                    if (searchValue.Value != i * 10 + 5)
                    {
                        Console.WriteLine($"3: {searchValue.Second} - {i * 10 + 5}");
                        Assert.Fail("Wide key doesn't work");
                    }
                }

                for (int i = 1; i <= count; i++)
                {
                    var searchValue = new DupValueWithWideKey()
                    {
                        First = 0,
                        Second = (ulong)i * 10 + 6, // +10, always larger
                        Value = i * 10 + 5
                    };

                    c.TryFindDup(Lookup.LT, ref nodupKey, ref searchValue);

                    if (searchValue.Value != i * 10 + 5)
                    {
                        Console.WriteLine($"4: {searchValue.Value} - {i * 10 + 5}");
                        Assert.Fail("Wide key doesn't work");
                    }
                }
            }

            db.Dispose();
            env.Close();
        }

        [Test, Explicit("TODO")]
        public void CouldFindDupWideKey()
        {
            var path = TestUtils.GetPath();
            var env = LMDBEnvironment.Create(path,
                 LMDBEnvironmentFlags.NoSync, disableAsync: true);
            env.MapSize = 100 * 1024 * 1024;
            env.Open();

            var db = env.OpenDatabase("CouldFindDupWideKey",
                new DatabaseConfig(DbFlags.Create | DbFlags.DuplicatesFixed)
                {
                    DupSortPrefix = 64 * 64
                });
            db.Truncate();

            var nodupKey = 0;

            var count = 500;

            {
                try
                {
                    for (int i = 1; i <= count; i++)
                    {
                        var dupValue = new DupValueWithWideKey()
                        {
                            First = (ulong)i,
                            Second = (ulong)i * 10 + 5,
                            Value = i * 10 + 5
                        };
                        db.Put(nodupKey, dupValue, TransactionPutOptions.AppendDuplicateData);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                }
            }

            var rounds = 1;
            for (int r = 0; r < rounds; r++)
            {
                using (Benchmark.Run("WideKeyFind", count * 5))
                {
                    for (int i = 1; i <= count; i++)
                    {
                        var searchValue = new DupValueWithWideKey()
                        {
                            First = 0,
                            Second = (ulong)i * 10, // without + 5, always smaller
                            Value = i * 10 + 5
                        };
                        using (var txn = env.BeginReadOnlyTransaction())
                        using (var c = db.OpenReadOnlyCursor(txn))
                        {
                            c.TryFindDup(Lookup.GT, ref nodupKey, ref searchValue);

                            if (searchValue.Value != i * 10 + 5)
                            {
                                Console.WriteLine($"2: {searchValue.Value} - {i * 10 + 5}");
                                Assert.Fail("Wide key doesn't work");
                            }
                        }
                    }

                    for (int i = 1; i <= count; i++)
                    {
                        var searchValue = new DupValueWithWideKey()
                        {
                            First = 0,
                            Second = (ulong)i * 10 + 5, // equal
                            Value = i * 10 + 5
                        };

                        using (var txn = env.BeginReadOnlyTransaction())
                        using (var c = db.OpenReadOnlyCursor(txn))
                        {
                            c.TryFindDup(Lookup.EQ, ref nodupKey, ref searchValue);

                            if (searchValue.Value != i * 10 + 5)
                            {
                                Console.WriteLine($"2: {searchValue.Value} - {i * 10 + 5}");
                                Assert.Fail("Wide key doesn't work");
                            }
                        }

                        searchValue = new DupValueWithWideKey()
                        {
                            First = 0,
                            Second = (ulong)i * 10 + 5, // equal
                            Value = i * 10 + 5
                        };

                        using (var txn = env.BeginReadOnlyTransaction())
                        using (var c = db.OpenReadOnlyCursor(txn))
                        {
                            c.TryFindDup(Lookup.GE, ref nodupKey, ref searchValue);

                            if (searchValue.Value != i * 10 + 5)
                            {
                                Console.WriteLine($"2: {searchValue.Value} - {i * 10 + 5}");
                                Assert.Fail("Wide key doesn't work");
                            }
                        }

                        searchValue = new DupValueWithWideKey()
                        {
                            First = 0,
                            Second = (ulong)i * 10 + 5, // equal
                            Value = i * 10 + 5
                        };
                        using (var txn = env.BeginReadOnlyTransaction())
                        using (var c = db.OpenReadOnlyCursor(txn))
                        {
                            if (!c.TryFindDup(Lookup.LE, ref nodupKey, ref searchValue))
                            {
                                Assert.Fail("Cannot find");
                            }

                            if (searchValue.Value != i * 10 + 5)
                            {
                                Console.WriteLine($"3: {searchValue.Second} - {i * 10 + 5}");
                                Assert.Fail("Wide key doesn't work");
                            }
                        }
                    }

                    for (int i = 1; i <= count; i++)
                    {
                        var searchValue = new DupValueWithWideKey()
                        {
                            First = 0,
                            Second = (ulong)i * 10 + 6, // +10, always larger
                            Value = i * 10 + 5
                        };
                        using (var txn = env.BeginReadOnlyTransaction())
                        using (var c = db.OpenReadOnlyCursor(txn))
                        {
                            c.TryFindDup(Lookup.LT, ref nodupKey, ref searchValue);

                            if (searchValue.Value != i * 10 + 5)
                            {
                                Console.WriteLine($"4: {searchValue.Value} - {i * 10 + 5}");
                                Assert.Fail("Wide key doesn't work");
                            }
                        }
                    }
                }

                using (Benchmark.Run("WideKeyFind SC", count * 5))

                {
                    for (int i = 1; i <= count; i++)
                    {
                        var searchValue = new DupValueWithWideKey()
                        {
                            First = 0,
                            Second = (ulong)i * 10, // without + 5, always smaller
                            Value = i * 10 + 5
                        };

                        using (var txn = env.BeginReadOnlyTransaction())
                        {
                            db.TryFindDup(txn, Lookup.GT, ref nodupKey, ref searchValue);

                            if (searchValue.Value != i * 10 + 5)
                            {
                                Console.WriteLine($"2: {searchValue.Value} - {i * 10 + 5}");
                                Assert.Fail("Wide key doesn't work");
                            }
                        }
                    }

                    for (int i = 1; i <= count; i++)
                    {
                        var searchValue = new DupValueWithWideKey()
                        {
                            First = 0,
                            Second = (ulong)i * 10 + 5, // equal
                            Value = i * 10 + 5
                        };

                        using (var txn = env.BeginReadOnlyTransaction())
                        {
                            db.TryFindDup(txn, Lookup.EQ, ref nodupKey, ref searchValue);

                            if (searchValue.Value != i * 10 + 5)
                            {
                                Console.WriteLine($"2: {searchValue.Value} - {i * 10 + 5}");
                                Assert.Fail("Wide key doesn't work");
                            }
                        }

                        searchValue = new DupValueWithWideKey()
                        {
                            First = 0,
                            Second = (ulong)i * 10 + 5, // equal
                            Value = i * 10 + 5
                        };

                        using (var txn = env.BeginReadOnlyTransaction())
                        {
                            db.TryFindDup(txn, Lookup.GE, ref nodupKey, ref searchValue);

                            if (searchValue.Value != i * 10 + 5)
                            {
                                Console.WriteLine($"2: {searchValue.Value} - {i * 10 + 5}");
                                Assert.Fail("Wide key doesn't work");
                            }
                        }

                        searchValue = new DupValueWithWideKey()
                        {
                            First = 0,
                            Second = (ulong)i * 10 + 5, // equal
                            Value = i * 10 + 5
                        };

                        using (var txn = env.BeginReadOnlyTransaction())
                        {
                            if (!db.TryFindDup(txn, Lookup.LE, ref nodupKey, ref searchValue))
                            {
                                Assert.Fail("Cannot find");
                            }

                            if (searchValue.Value != i * 10 + 5)
                            {
                                Console.WriteLine($"3: {searchValue.Second} - {i * 10 + 5}");
                                Assert.Fail("Wide key doesn't work");
                            }
                        }
                    }

                    for (int i = 1; i <= count; i++)
                    {
                        var searchValue = new DupValueWithWideKey()
                        {
                            First = 0,
                            Second = (ulong)i * 10 + 6, // +10, always larger
                            Value = i * 10 + 5
                        };

                        using (var txn = env.BeginReadOnlyTransaction())
                        {
                            db.TryFindDup(txn, Lookup.LT, ref nodupKey, ref searchValue);

                            if (searchValue.Value != i * 10 + 5)
                            {
                                Console.WriteLine($"4: {searchValue.Value} - {i * 10 + 5}");
                                Assert.Fail("Wide key doesn't work");
                            }
                        }
                    }
                }
            }

            Benchmark.Dump();

            db.Dispose();

            env.Close();
        }

        [StructLayout(LayoutKind.Explicit, Size = 32)]
        public struct MyDupSorted
        {
            [FieldOffset(0)]
            public ulong Key;

            [FieldOffset(8)]
            public long Value1;

            [FieldOffset(16)]
            public long Value2;

            [FieldOffset(24)]
            public uint Value3;

            [FieldOffset(28)]
            public int Value4;
        }

        [Test]
        public void CouldFindDupDSIssue()
        {
            var env = LMDBEnvironment.Create("./Data/CouldFindDup",
                LMDBEnvironmentFlags.WriteMap | LMDBEnvironmentFlags.NoSync);
            env.Open();

            var db = env.OpenDatabase("_streamLogs",
                new DatabaseConfig(DbFlags.Create | DbFlags.IntegerDuplicates | DbFlags.DuplicatesFixed | DbFlags.IntegerKey)
                {
                    DupSortPrefix = 64
                });
            // db.Truncate();

            var nodupKey = 10000;
            var count = 2;
            for (int i = 0; i < count; i++)
            {
                var lr1 = new MyDupSorted() { Key = (ulong)i + 1 };
                db.Put(nodupKey, lr1);

                using (var txn = env.BeginReadOnlyTransaction())
                {
                    var c = db.OpenReadOnlyCursor(txn);
                    var searchValue = new MyDupSorted() { Key = ulong.MaxValue };
                    var found = c.TryFindDup(Lookup.LT, ref nodupKey, ref searchValue);
                    Assert.IsTrue(found);
                    c.Dispose();
                }
            }
            db.Dispose();
            env.Close();
        }
    }
}