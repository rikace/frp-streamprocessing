using System;
using System.Collections.Generic;
using System.IO;

namespace WebCrawler
{
    class Program
    {
        static void Main(string[] args)
        {
            var urls = new List<string>();

            DataFlowCrawler.Start(urls, async (url, buffer) =>
            {
                string fileName = Path.GetFileName(url);
                string name = @"Images\" + fileName;

                using (Stream srm = File.OpenWrite(name))
                {
                    await srm.WriteAsync(buffer, 0, buffer.Length);
                }
            });

            Console.ReadLine();
        }
    }
}