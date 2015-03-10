using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DownloadMetrics
{
    class Program
    {
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
                while(!streamReader.EndOfStream)
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
            Process p = new Process();
            p.StartInfo.UseShellExecute = false;

            p.StartInfo.FileName = "cmd.exe";
            p.StartInfo.Arguments = String.Format("/C {0}", path);
            p.Start();

            p.WaitForExit();
            double result = DateTime.Now.Subtract(startTime).TotalMilliseconds;
            Console.WriteLine("ending {0} took {1} ms", path, result);
            AddResult(result);
        }

        static void Main(string[] args)
        {
            if (args.Length < 2)
            {
                Console.WriteLine("Nothing to do.");
                Console.WriteLine("Usage: DownloadMetrics.exe [serial|parallel|tasks] <file containing paths of batch files>");
                return;
            }

            List<string> paths = ReadLines(args[1]).ToList();

            DateTime startTime = DateTime.Now;

            if (args[0] == "serial")
            {
                foreach (string path in paths)
                {
                    Run(path);
                }
            }
            else if (args[0] == "parallel")
            {
                List<Action> actions = new List<Action>();
                foreach (string path in paths)
                {
                    string p = path;
                    actions.Add(
                        () => {
                            Run(p);
                        });
                }

                Parallel.Invoke(new ParallelOptions
                {
                    MaxDegreeOfParallelism = paths.Count
                }, actions.ToArray());
            }
            else if (args[0] == "tasks")
            {
                List<Task> tasks = paths.Select(p => Task.Factory.StartNew(() => { Run(p); }, TaskCreationOptions.LongRunning)).ToList();
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
                File.WriteAllText("results.csv", String.Join(",", _results.Select(r => r.ToString())));
            }
        }
    }
}
