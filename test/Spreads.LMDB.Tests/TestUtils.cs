using System.IO;
using System.Runtime.CompilerServices;

namespace Spreads.LMDB.Tests
{
    public static class TestUtils
    {
        public static string BaseDataPath = "./Data/tmp/TestData/";

        public static void ClearAll([CallerFilePath]string groupPath = null)
        {
            var group = Path.GetFileNameWithoutExtension(groupPath);
            var path = Path.Combine(BaseDataPath, group);
            if (Directory.Exists(path))
            {
                Directory.Delete(path, true);
            }
        }

        public static string GetPath(
            [CallerMemberName]string testPath = null,
            [CallerFilePath]string groupPath = null,
            bool clear = true)
        {
            var group = Path.GetFileNameWithoutExtension(groupPath);
            var path = Path.Combine(BaseDataPath, group, testPath);
            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }
            else if (clear)
            {
                var di = new DirectoryInfo(path);

                foreach (FileInfo file in di.GetFiles())
                {
                    file.Delete();
                }
                foreach (DirectoryInfo dir in di.GetDirectories())
                {
                    dir.Delete(true);
                }
            }

            return path;
        }
    }
}
