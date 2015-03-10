using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;

namespace DownloadMetricsSDK
{
    class Program
    {
        private static string _bucketName = "wdl-s3-tests";

        private static IAmazonS3 _ambientClient;

        private static readonly object _resultLock = new object();

        private static List<double> _results = new List<double>();

        private static void AddResult(double result)
        {
            lock (_resultLock)
            {
                _results.Add(result);
            }
        }

        public static IEnumerable<string> ReadLines(string pathname)
        {
            List<string> lines = new List<string>();
            using (StreamReader streamReader = new StreamReader(pathname))
            {
                while (!streamReader.EndOfStream)
                {
                    string line = streamReader.ReadLine();
                    lines.Add(line);
                    //Console.WriteLine("read: {0}", line);
                }
                streamReader.Close();
            }

            return lines;
        }

        private static void Run(string path)
        {
            DateTime startTime = DateTime.Now;
            //Console.WriteLine("running {0}", path);

            using (IAmazonS3 transport = new AmazonS3Client(Amazon.RegionEndpoint.EUWest1))
            {
                GetObjectRequest request = new GetObjectRequest
                {
                    BucketName = _bucketName,
                    Key = path
                };

                using (GetObjectResponse response = transport.GetObject(request))
                {
                    string dest = String.Concat(Path.Combine(Environment.CurrentDirectory, "s3", Guid.NewGuid().ToString()), ".jp2");
                    if (File.Exists(dest))
                    {
                        File.Delete(dest);
                    }
                    response.WriteResponseStreamToFile(dest);
                }
            }


            double result = DateTime.Now.Subtract(startTime).TotalMilliseconds;
            AddResult(result);
            Console.WriteLine("ending {0} took {1} ms", path, result);
        }

        private static void RunWithAmbient(string path)
        {
            DateTime startTime = DateTime.Now;
            //Console.WriteLine("running {0}", path);

            GetObjectRequest request = new GetObjectRequest
            {
                BucketName = _bucketName,
                Key = path
            };

            using (GetObjectResponse response = _ambientClient.GetObject(request))
            {
                string dest = String.Concat(Path.Combine(Environment.CurrentDirectory, "s3", Guid.NewGuid().ToString()), ".jp2");
                if (File.Exists(dest))
                {
                    File.Delete(dest);
                }
                response.WriteResponseStreamToFile(dest);
            }

            double result = DateTime.Now.Subtract(startTime).TotalMilliseconds;
            AddResult(result);
            Console.WriteLine("ending {0} took {1} ms", path, result);
        }

        static void Main(string[] args)
        {
            if (args.Length < 2)
            {
                Console.WriteLine("Nothing to do.");
                Console.WriteLine("Usage: DownloadMetricsSDK.exe [serial|parallel|tasks] <file containing list of s3 keys>");
                return;
            }

            if (args[0] == "setup")
            {
                string sourceKey = args[1];
                int copies = Convert.ToInt32(args[2]);

                using (IAmazonS3 transport = new AmazonS3Client(Amazon.RegionEndpoint.EUWest1))
                {
                    List<Action> actions = new List<Action>();
                    for (int x = 0; x < copies; x++)
                    {
                        int closureX = x;
                        actions.Add(new Action(() =>
                        {
                            string s = String.Concat(closureX.ToString(), "-", sourceKey);
                            Console.WriteLine("Copying {0} to {1} ...", sourceKey, s);
                            transport.CopyObject(_bucketName, sourceKey, _bucketName, s);
                            Console.WriteLine("Copied {0} to {1}", sourceKey, s);
                        }));
                    }
                    Parallel.Invoke(new ParallelOptions { MaxDegreeOfParallelism = copies }, actions.ToArray());
                }

                return;
            }

            foreach (string filename in Directory.EnumerateFiles("s3", "*.jp2"))
            {
                //Console.WriteLine("Removing {0}", filename);
                File.Delete(filename);
            }

            using (_ambientClient = new AmazonS3Client(Amazon.RegionEndpoint.EUWest1))
            {
                using (GetObjectResponse response = _ambientClient.GetObject(_bucketName, "hello.txt"))
                {
                    if (File.Exists("hello.txt"))
                    {
                        File.Delete("hello.txt");
                    }
                    response.WriteResponseStreamToFile("hello.txt");
                }
                
                List<string> paths = ReadLines(args[1]).ToList();

                DateTime startTime = DateTime.Now;

                if (args[0] == "serial")
                {
                    foreach (string path in paths)
                    {
                        if (_ambientClient != null)
                        {
                            RunWithAmbient(path);
                        }
                        else
                        {
                            Run(path);
                        }
                    }
                }
                else if (args[0] == "parallel")
                {
                    List<Action> actions = new List<Action>();
                    foreach (string path in paths)
                    {
                        string p = path;
                        actions.Add(
                            () =>
                            {
                                if (_ambientClient != null)
                                {
                                    RunWithAmbient(path);
                                }
                                else
                                {
                                    Run(path);
                                }
                            });
                    }

                    Parallel.Invoke(new ParallelOptions
                    {
                        MaxDegreeOfParallelism = paths.Count
                    }, actions.ToArray());
                }
                else if (args[0] == "tasks")
                {
                    List<Task> tasks =
                        paths.Select(p => Task.Factory.StartNew(() => { Run(p); }, TaskCreationOptions.LongRunning))
                            .ToList();
                    Task.WaitAll(tasks.ToArray());
                }

                TimeSpan ts = DateTime.Now.Subtract(startTime);

                Console.WriteLine("total {0} ms", ts.TotalMilliseconds);

                lock (_resultLock)
                {
                    _results.Add(ts.TotalMilliseconds);
                    double avg = _results.Sum() / _results.Count;
                    _results.Add(avg);
                    Console.WriteLine("avg = {0} ms", avg);
                    File.AppendAllText("results.csv", String.Concat(String.Join(",", _results.Select(r => r.ToString())), "\r\n"));
                }
            }
        }
    }
}
