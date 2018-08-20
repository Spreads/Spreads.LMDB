using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
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
        private static async Task Main(string[] args)
        {
            Trace.Listeners.Add(new ConsoleListener());

            var tests = new SpreadsMethodsTests();
            tests.CouldFindDup();
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
