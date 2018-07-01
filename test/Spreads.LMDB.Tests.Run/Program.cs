using System;
using System.Diagnostics;

namespace Spreads.LMDB.Tests.Run
{
    class ConsoleListener : TraceListener
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
    class Program
    {
        static void Main(string[] args)
        {
            Trace.Listeners.Add(new ConsoleListener());
            try
            {
                (new LMDBTests()).CouldCreateEnvironment().Wait();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }

            Console.WriteLine("Press Enter to exit...");
            Console.ReadLine();
        }
    }
}
