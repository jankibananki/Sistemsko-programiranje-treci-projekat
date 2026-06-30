using Akka.Actor;
using Newtonsoft.Json.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace Projekat3
{
    // Rx sloj: periodično poziva eksterni News API, mapira title/source i emituje poruke ka aktorima.
    class Article : IObservable<NewsBatch>, IDisposable
    {
        private readonly ISubject<NewsBatch> _subject;
        private readonly System.Reactive.Concurrency.IScheduler _scheduler;
        private readonly IActorRef _newsActor;
        private readonly object _lockConsole;
        private readonly HttpClient _client;
        private IDisposable? _polling;

        public Article(IActorRef newsActor, object lockConsole)
        {
            _newsActor = newsActor;
            _lockConsole = lockConsole;
            _subject = new Subject<NewsBatch>();
            _scheduler = new EventLoopScheduler();
            _client = new HttpClient();
            _client.DefaultRequestHeaders.Add("User-Agent", "Projekat3 News API console server");
        }

        public IDisposable Subscribe(IObserver<NewsBatch> observer)
        {
            return _subject.ObserveOn(_scheduler).Subscribe(observer);
        }

        public void Start(string apiKey, TimeSpan period)
        {
            if (_polling != null)
                return;

            _client.DefaultRequestHeaders.Remove("X-Api-Key");
            _client.DefaultRequestHeaders.Add("X-Api-Key", apiKey);

            _polling = Observable
                .Timer(TimeSpan.Zero, period)
                .SelectMany(_ => Observable.FromAsync(PollOnce))
                .SelectMany(batches => batches.ToObservable())
                .Subscribe(
                    batch => _subject.OnNext(batch),
                    error => WriteToConsole("Rx error: " + error.Message));
        }

        private async Task<IReadOnlyList<NewsBatch>> PollOnce()
        {
            try
            {
                TrackedQueries tracked = await _newsActor.Ask<TrackedQueries>(new GetTrackedQueries(), TimeSpan.FromSeconds(5));
                List<NewsBatch> batches = new List<NewsBatch>();

                foreach (NewsQuery query in tracked.Queries)
                {
                    IReadOnlyList<NewsItem>? articles = await GetArticles(query.Keyword, query.Category);
                    if (articles != null)
                        batches.Add(new NewsBatch(query.Keyword, query.Category, articles));
                }

                return batches;
            }
            catch (Exception error)
            {
                WriteToConsole("Rx polling failed: " + error.Message);
                return Array.Empty<NewsBatch>();
            }
        }

        private async Task<IReadOnlyList<NewsItem>?> GetArticles(string keyword, string category)
        {
            string url = "https://newsapi.org/v2/top-headlines"
                         + "?q=" + Uri.EscapeDataString(keyword)
                         + "&category=" + Uri.EscapeDataString(category)
                         + "&pageSize=30";

            try
            {
                JArray articles = await Observable
                    .FromAsync(async cancellationToken =>
                    {
                        using HttpResponseMessage response = await _client.GetAsync(url, cancellationToken);
                        string responseContent = await response.Content.ReadAsStringAsync(cancellationToken);

                        if (!response.IsSuccessStatusCode)
                            throw new Exception("News API error: " + response.StatusCode + " " + responseContent);

                        JObject json = JObject.Parse(responseContent);
                        return (JArray?)json["articles"] ?? new JArray();
                    })
                    .FirstAsync();

                return articles
                    .ToObservable(ThreadPoolScheduler.Instance)
                    .Select(token => new NewsItem(
                        token["title"]?.ToString() ?? "",
                        token["source"]?["name"]?.ToString() ?? "Unknown"))
                    .Where(item => !string.IsNullOrWhiteSpace(item.Title))
                    .Where(item => !string.IsNullOrWhiteSpace(item.Source))
                    .ToEnumerable()
                    .ToList();
            }
            catch (Exception error)
            {
                WriteToConsole("News API polling failed for " + keyword + "/" + category + ": " + error.Message);
                return null;
            }
        }

        public void Dispose()
        {
            _polling?.Dispose();
            (_subject as IDisposable)?.Dispose();
            (_scheduler as IDisposable)?.Dispose();
            _client.Dispose();
        }

        private void WriteToConsole(string message)
        {
            lock (_lockConsole)
            {
                Console.WriteLine(message);
            }
        }
    }

    class NewsBatch
    {
        public string Keyword { get; }
        public string Category { get; }
        public IReadOnlyList<NewsItem> Articles { get; }

        public NewsBatch(string keyword, string category, IReadOnlyList<NewsItem> articles)
        {
            Keyword = keyword;
            Category = category;
            Articles = articles;
        }
    }

    class NewsItem
    {
        public string Title { get; }
        public string Source { get; }

        public NewsItem(string title, string source)
        {
            Title = title;
            Source = source;
        }
    }
}
