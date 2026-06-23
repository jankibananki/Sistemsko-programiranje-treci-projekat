using Newtonsoft.Json.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace Projekat3
{
    // Rx sloj: poziva eksterni News API, mapira title/source i emituje poruke ka aktorima.
    class Article : IObservable<NewsItem>, IDisposable
    {
        private readonly ISubject<NewsItem> _subject;
        private readonly IScheduler _scheduler;

        public Article()
        {
            _subject = new Subject<NewsItem>();
            _scheduler = new EventLoopScheduler();
        }

        public IDisposable Subscribe(IObserver<NewsItem> observer)
        {
            return _subject.ObserveOn(_scheduler).Subscribe(observer);
        }

        public async Task GetArticles(long requestId, string keyword, string category, string apiKey)
        {
            try
            {
                string url = "https://newsapi.org/v2/top-headlines"
                             + "?q=" + Uri.EscapeDataString(keyword)
                             + "&category=" + Uri.EscapeDataString(category)
                             + "&pageSize=30";

                var observable = Observable
                    .FromAsync(async cancellationToken =>
                    {
                        using HttpClient client = new HttpClient();
                        client.DefaultRequestHeaders.Add("User-Agent", "Projekat3 News API console server");
                        client.DefaultRequestHeaders.Add("X-Api-Key", apiKey);

                        using HttpResponseMessage response = await client.GetAsync(url, cancellationToken);
                        string responseContent = await response.Content.ReadAsStringAsync(cancellationToken);

                        if (!response.IsSuccessStatusCode)
                            throw new Exception("News API error: " + response.StatusCode + " " + responseContent);

                        JObject json = JObject.Parse(responseContent);
                        return (JArray?)json["articles"] ?? new JArray();
                    })
                    .SelectMany(articles => articles.ToObservable(ThreadPoolScheduler.Instance))
                        .Select(token => new NewsItem(
                            requestId,
                            token["title"]?.ToString() ?? "",
                            token["source"]?["name"]?.ToString() ?? "Unknown"))
                        .Where(item => !string.IsNullOrWhiteSpace(item.Title))
                        .Where(item => !string.IsNullOrWhiteSpace(item.Source));

                TaskCompletionSource<bool> done = new TaskCompletionSource<bool>();

                using (observable.Subscribe(
                    item => _subject.OnNext(item),
                    error => done.TrySetException(error),
                    () => done.TrySetResult(true)))
                {
                    await done.Task;
                }

                _subject.OnCompleted();
            }
            catch (Exception e)
            {
                _subject.OnError(e);
            }
        }

        public void Dispose()
        {
            (_subject as IDisposable)?.Dispose();
            (_scheduler as IDisposable)?.Dispose();
        }
    }

    class NewsItem
    {
        public long RequestId { get; }
        public string Title { get; }
        public string Source { get; }

        public NewsItem(long requestId, string title, string source)
        {
            RequestId = requestId;
            Title = title;
            Source = source;
        }
    }
}
