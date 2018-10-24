// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

using NUnit.Framework;
using Spreads.Utils;
using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Spreads.LMDB.Tests
{
    [TestFixture]
    public class PerfTests
    {
        [Test, Explicit("long running")]
        public unsafe void SimpleWriteReadBenchmark()
        {
#pragma warning disable 618
            Settings.DoAdditionalCorrectnessChecks = false;
#pragma warning restore 618

            var count = 1_00_000;
            var rounds = 10;

            var path = TestUtils.GetPath();
            if (Directory.Exists(path))
            {
                Directory.Delete(path, true);
            }

            var dirS = Path.Combine(path, "Spreads");
            var dirK = Path.Combine(path, "KdSoft");
            Directory.CreateDirectory(dirS);
            Directory.CreateDirectory(dirK);

            var envS = LMDBEnvironment.Create(dirS, DbEnvironmentFlags.NoSync,
                disableAsync: true);
            envS.MaxDatabases = 10;
            envS.MapSize = 256 * 1024 * 1024;
            envS.Open();
            var dbS = envS.OpenDatabase("SimpleWrite", new DatabaseConfig(DbFlags.Create | DbFlags.IntegerKey));

            for (int r = 0; r < rounds; r++)
            {
                using (Benchmark.Run("Spreads Write", count, true))
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
           
            Benchmark.Dump("SimpleWrite/Read 10x1M longs");
        }

        [Test, Explicit("long running")]
        public unsafe void SimpleWriteReadBatchedBenchmark()
        {
#pragma warning disable 618
            Settings.DoAdditionalCorrectnessChecks = false;
#pragma warning restore 618
            var count = 1_00_000;
            var rounds = 10;

            var path = TestUtils.GetPath();
            if (Directory.Exists(path))
            {
                Directory.Delete(path, true);
            }

            var dirS = Path.Combine(path, "Spreads");
            var dirK = Path.Combine(path, "KdSoft");
            Directory.CreateDirectory(dirS);
            Directory.CreateDirectory(dirK);

            var envS = LMDBEnvironment.Create(dirS, DbEnvironmentFlags.NoSync, disableAsync: true);
            envS.MaxDatabases = 10;
            envS.MapSize = 256 * 1024 * 1024;
            envS.Open();
            var dbS = envS.OpenDatabase("SimpleWrite", new DatabaseConfig(DbFlags.Create | DbFlags.IntegerKey));

            

            // var garbage1 = new byte[1];

            for (int r = 0; r < rounds; r++)
            {
                using (Benchmark.Run("Spreads Write", count, true))
                {
                    using (var tx = envS.BeginTransaction())
                    {
                        for (long i = r * count; i < (r + 1) * count; i++)
                        {
                            dbS.Put(tx, i, i, TransactionPutOptions.AppendData);
                        }

                        tx.Commit();
                    }
                }

                var extraRounds = 100;
                using (Benchmark.Run("Spreads Read", count * extraRounds, true))
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
            var count = 5_00;
            var rounds = 1;

            var path = TestUtils.GetPath();
            if (Directory.Exists(path))
            {
                Directory.Delete(path, true);
            }

            var dirS = Path.Combine(path, "Spreads");
            var dirK = Path.Combine(path, "KdSoft");
            Directory.CreateDirectory(dirS);
            Directory.CreateDirectory(dirK);

            var envS = LMDBEnvironment.Create(dirS, DbEnvironmentFlags.WriteMap | DbEnvironmentFlags.NoMetaSync,
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