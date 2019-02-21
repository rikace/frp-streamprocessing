namespace WebCrawler
{
    using System;
    using System.Linq;
    using HtmlAgilityPack;
    using System.Collections.Generic;
    using System.IO;
    using System.Net;
    using System.Reactive.Disposables;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Threading.Tasks.Dataflow;
    using System.Text.RegularExpressions;

    public static class DataFlowScraper
    {
        private static void WriteLineInColor(string message, ConsoleColor foregroundColor)
        {
            Console.ForegroundColor = foregroundColor;
            Console.WriteLine(message);
            Console.ResetColor();
        }

        private static readonly Regex httpRgx = new Regex(@"^(http|https|www)://.*$");

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
                    var doc = new HtmlDocument();
                    doc.LoadHtml(html);

                    var links = from n in doc.DocumentNode.Descendants("a")
                        where n.Attributes.Contains("href")
                        let url = n.GetAttributeValue("href", "")
                        where httpRgx.IsMatch(url)
                        select url;

                    return links;
                });

            var imgParser = new TransformManyBlock<string, string>(
                (html) =>
                {
                    var doc = new HtmlDocument();
                    doc.LoadHtml(html);

                    var imageLinks = from n in doc.DocumentNode.Descendants("img")
                        where n.Attributes.Contains("src")
                        let url = n.GetAttributeValue("src", "")
                        where httpRgx.IsMatch(url)
                        select url;
                    return imageLinks;
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