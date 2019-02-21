using System.Collections.Concurrent;
using System.Linq;

namespace WebCrawler
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Net;
    using System.Reactive.Disposables;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Threading.Tasks.Dataflow;
    using System.Text.RegularExpressions;
        
    public static class DataFlowCrawler
    {
        private static ConsoleColor[] colors = new ConsoleColor[]
        {
            ConsoleColor.Black,
            ConsoleColor.DarkBlue,
            ConsoleColor.DarkGreen,
            ConsoleColor.DarkCyan,
            ConsoleColor.DarkRed,
            ConsoleColor.DarkMagenta,
            ConsoleColor.DarkYellow,
            ConsoleColor.Gray,
            ConsoleColor.DarkGray,
            ConsoleColor.Blue,
            ConsoleColor.Green,
            ConsoleColor.Cyan,
            ConsoleColor.Red,
            ConsoleColor.Magenta,
            ConsoleColor.Yellow,
            ConsoleColor.White
        };
        static int index = 0;
        static ConcurrentDictionary<int, ConsoleColor> mapColors = new ConcurrentDictionary<int, ConsoleColor>();
        private static ConsoleColor ColorByInt(int id)
        {   
            
            return mapColors.GetOrAdd(id, _ => colors[Interlocked.Increment(ref index) % (colors.Length - 1)]);
            
        }
        
        private static void WriteLineInColor(string message, ConsoleColor foregroundColor)
        {
            Console.ForegroundColor = foregroundColor;
            Console.WriteLine(message);
            Console.ResetColor();
        }

        private const string LINK_REGEX_HREF = "\\shref=('|\\\")?(?<LINK>http\\://.*?(?=\\1)).*>";
        private static readonly Regex _linkRegexHRef = new Regex(LINK_REGEX_HREF);
        private const string IMG_REGEX = "<\\s*img [^\\>]*src=('|\")?(?<IMG>http\\://.*?(?=\\1)).*>\\s*([^<]+|.*?)?\\s*</a>";
        private static readonly Regex _imgRegex = new Regex(IMG_REGEX);

        public static IDisposable Start(List<string> urls, Func<string, byte[], Task> compute)
        {
            // Step 1
            var downloaderOptions = new ExecutionDataflowBlockOptions() { };
            var downloader = new TransformBlock<string, string>(
                async (url) =>
                {
                    // using IOCP the thread pool worker thread does return to the pool
                    WebClient wc = new WebClient();
                    string result = await wc.DownloadStringTaskAsync(url);
                    return result;
                }, downloaderOptions);


            var printer = new ActionBlock<string>(text =>
            {         
                Console.WriteLine($"Recveied text - Thread ID {Thread.CurrentThread.ManagedThreadId}");
            });

            downloader.LinkTo(printer);

            foreach (var url in urls)
            {
                downloader.Post(url);
            }

            // Step 2
            var contentBroadcaster = new BroadcastBlock<string>(s => s);

            var linkParser = new TransformManyBlock<string, string>(
                (html) =>
                {
                    var output = new List<string>();
                    var links = _linkRegexHRef.Matches(html);
                    foreach (Match item in links)
                    {
                        var value = item.Groups["LINK"].Value;
                        output.Add(value);
                    }

                    return output;
                });

            var imgParser = new TransformManyBlock<string, string>(
                (html) =>
                {
                    var output = new List<string>();
                    var images = _imgRegex.Matches(html);
                    foreach (Match item in images)
                    {
                        var value = item.Groups["IMG"].Value;
                        output.Add(value);
                    }

                    return output;
                });


            // Step 3

            var linkBroadcaster = new BroadcastBlock<string>(s => s);

            var writer = new ActionBlock<string>(async url =>
            {
                WebClient wc = new WebClient();
                // using IOCP the thread pool worker thread does return to the pool
                byte[] buffer = await wc.DownloadDataTaskAsync(url);
                
                await compute(url, buffer);
            });

            StringComparison comparison = StringComparison.InvariantCultureIgnoreCase;
            Predicate<string> linkFilter = link =>
                link.IndexOf(".aspx", comparison) != -1 ||
                link.IndexOf(".php", comparison) != -1 ||
                link.IndexOf(".htm", comparison) != -1 ||
                link.IndexOf(".html", comparison) != -1;

            Predicate<string> imgFilter = url =>
                url.EndsWith(".jpg", comparison) ||
                url.EndsWith(".png", comparison) ||
                url.EndsWith(".gif", comparison);

            IDisposable disposeAll = new CompositeDisposable(
                // from [downloader] to [contentBroadcaster]
                downloader.LinkTo(contentBroadcaster),
                // from [contentBroadcaster] to [imgParser]
                contentBroadcaster.LinkTo(imgParser),
                // from [contentBroadcaster] to [linkParserHRef]
                contentBroadcaster.LinkTo(linkParser),
                // from [linkParser] to [linkBroadcaster]
                linkParser.LinkTo(linkBroadcaster),
                // conditional link to from [linkBroadcaster] to [downloader]
                linkBroadcaster.LinkTo(downloader, linkFilter),
                // from [linkBroadcaster] to [writer]
                linkBroadcaster.LinkTo(writer, imgFilter),
                // from [imgParser] to [writer]
                imgParser.LinkTo(writer));
            
            return disposeAll;
        }
    }
}
