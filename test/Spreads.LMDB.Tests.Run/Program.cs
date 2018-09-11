using Spreads.Utils;
using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;

namespace Spreads.LMDB.Tests.Run
{
    internal class ConsoleListener : TraceListener
    {
        public override void Write(string message)
        {
            Console.Write(message);
        }

        public override void WriteLine(string message)
        {
            Console.WriteLine(message);
        }
    }

    internal class Program
    {
        private static void Main(string[] args)
        {
            const string lmdbPath = "lmdbDatabase";
            const int hashTablesCount = 50;
            if (Directory.Exists(lmdbPath))
                Directory.Delete(lmdbPath, true);
            Directory.CreateDirectory(lmdbPath);
            using (var environment = LMDBEnvironment.Create(lmdbPath, DbEnvironmentFlags.None))
            {
                environment.MapSize = (1024L * 1024L * 1024L * 10L); // 10 GB
                environment.MaxDatabases = hashTablesCount + 2;
                environment.MaxReaders = 1000;
                environment.Open();

                // Open all database to make sure they exists
                // using (var tx = environment.BeginTransaction())
                {
                    var configuration = new DatabaseConfig(DbFlags.Create | DbFlags.IntegerKey);

                    var tracksDatabase = environment.OpenDatabase("tracks", configuration);
                    var subFingerprintsDatabase = environment.OpenDatabase("subFingerprints", configuration);

                    var hashTables = new Database[hashTablesCount];
                    var hashTableConfig = new DatabaseConfig(
                        DbFlags.Create
                        | DbFlags.IntegerKey
                        | DbFlags.DuplicatesSort
                        | DbFlags.DuplicatesFixed
                        | DbFlags.IntegerDuplicates
                    );
                    for (int i = 0; i < hashTablesCount; i++)
                    {
                        hashTables[i] = environment.OpenDatabase($"HashTable{i}", hashTableConfig);
                    }

                    // tx.Commit();
                }
            }
        }

        private static async Task Main2(string[] args)
        {
            Trace.Listeners.Add(new ConsoleListener());

            var tests = new SpreadsMethodsTests();

            tests.CouldFindDupWideKey();
            
            Console.WriteLine("Finished");
            Console.ReadLine();
            //try
            //{
            //    //if (Directory.Exists("/localdata/shared_lmdb"))
            //    //{
            //    //    Directory.Delete("/localdata/shared_lmdb", true);
            //    //}
            //    var env = new LMDBEnvironment("/localdata/shared_lmdb", DbEnvironmentFlags.NoSync | DbEnvironmentFlags.WriteMap);
            //    env.MapSize = 512L * 1024 * 1024 * 1024;
            //    env.Open();

            //    // write 1M values and then read them

            //    var db = env.OpenDatabase("test", new DatabaseConfig(DbFlags.Create | DbFlags.IntegerKey)).Result;

            //    var count = 1_000_000;
            //    var lastValue = 0;
            //    var myupdates = 0;
            //    env.Write(txn => { db.Truncate(txn); txn.Commit(); });

            //    Thread.Sleep(5000);

            //    var sw = new Stopwatch();
            //    sw.Start();

            //    while (lastValue < count)
            //    {
            //        env.Write(txn =>
            //        {
            //            using (var c = db.OpenCursor(txn))
            //            {
            //                int key = default(int);

            //                if (c.TryGet(ref key, ref lastValue, CursorGetOption.Last))
            //                {
            //                    key++;
            //                    if (key - 1 == lastValue)
            //                    {
            //                        myupdates++;
            //                    }

            //                    lastValue = key;
            //                }

            //                c.Put(ref key, ref lastValue, CursorPutOptions.NoOverwrite);
            //                txn.Commit();
            //            }
            //        });
            //    }

            //    sw.Stop();
            //    env.Close().Wait();
            //    var elapsed = sw.ElapsedMilliseconds;
            //    File.WriteAllText($"/localdata/stat_{Process.GetCurrentProcess().Id}.txt", $"{myupdates} - {elapsed}");
            //}
            //catch (Exception ex)
            //{
            //    Console.WriteLine(ex);
            //}
        }
    }
}
