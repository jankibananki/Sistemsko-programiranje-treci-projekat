using Akka.Actor;
namespace Projekat3
{
    // Poruke koje web server i Rx tok šalju aktorima.
    record EnsureQuery(string Keyword, string Category);
    record GetResult(string Keyword, string Category);
    record GetTrackedQueries();
    record ArticlesUpdated(string Keyword, string Category, IReadOnlyList<NewsItem> Articles);
    record QueryReady(bool Ok);
    record TrackedQueries(IReadOnlyList<NewsQuery> Queries);
    record ArticlesAccepted(bool Ok);

    record NewsQuery(string Keyword, string Category);

    class NewsResult
    {
        public string Keyword { get; set; } = "";
        public string Category { get; set; } = "";
        public Dictionary<string, List<string>> TitlesBySource { get; set; } = new Dictionary<string, List<string>>();
        public List<Topic> Topics { get; set; } = new List<Topic>();
        public string SharpEntropyStatus { get; set; } = "";
    }

    class NewsActor : ReceiveActor
    {
        private sealed class QueryState
        {
            public string Keyword { get; }
            public string Category { get; }
            public Dictionary<string, List<string>> TitlesBySource { get; } =
                new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
            public List<string> AllTitles { get; } = new List<string>();

            public QueryState(string keyword, string category)
            {
                Keyword = keyword;
                Category = category;
            }
        }

        private readonly object _lockConsole;
        private readonly Dictionary<string, QueryState> _queries = new Dictionary<string, QueryState>(StringComparer.OrdinalIgnoreCase);

        public NewsActor(object lockConsole)
        {
            _lockConsole = lockConsole;

            Receive<EnsureQuery>(message =>
            {
                string key = CreateKey(message.Keyword, message.Category);
                if (!_queries.ContainsKey(key))
                {
                    _queries[key] = new QueryState(message.Keyword, message.Category);
                    WriteToConsole("Akka actor: prati se upit " + key);
                }

                Sender.Tell(new QueryReady(true));
            });

            Receive<GetTrackedQueries>(_ =>
            {
                IReadOnlyList<NewsQuery> queries = _queries.Values
                    .Select(state => new NewsQuery(state.Keyword, state.Category))
                    .ToList();

                Sender.Tell(new TrackedQueries(queries));
            });

            Receive<ArticlesUpdated>(message =>
            {
                string key = CreateKey(message.Keyword, message.Category);
                if (!_queries.TryGetValue(key, out QueryState? state))
                {
                    state = new QueryState(message.Keyword, message.Category);
                    _queries[key] = state;
                }

                state.TitlesBySource.Clear();
                state.AllTitles.Clear();

                foreach (NewsItem article in message.Articles)
                {
                    if (!state.TitlesBySource.TryGetValue(article.Source, out List<string>? titles))
                    {
                        titles = new List<string>();
                        state.TitlesBySource[article.Source] = titles;
                    }

                    titles.Add(article.Title);
                    state.AllTitles.Add(article.Title);
                }

                WriteToConsole("Akka actor: ažurirano stanje za upit " + key);
                Sender.Tell(new ArticlesAccepted(true));
            });

            Receive<GetResult>(message =>
            {
                string key = CreateKey(message.Keyword, message.Category);
                if (!_queries.TryGetValue(key, out QueryState? state))
                {
                    Sender.Tell(new Status.Failure(
                        new InvalidOperationException("Nepoznat upit: " + key)));
                    return;
                }

                Dictionary<string, List<string>> copy = state.TitlesBySource.ToDictionary(
                    pair => pair.Key,
                    pair => pair.Value.Distinct().OrderBy(title => title).ToList());

                TopicAnalysis analysis = TopicModeling.Run(state.AllTitles);

                NewsResult result = new NewsResult
                {
                    Keyword = state.Keyword,
                    Category = state.Category,
                    TitlesBySource = copy,
                    Topics = analysis.Topics,
                    SharpEntropyStatus = analysis.Status
                };

                Sender.Tell(result);
            });
        }

        private void WriteToConsole(string message)
        {
            lock (_lockConsole)
            {
                Console.WriteLine(message);
            }
        }

        private static string CreateKey(string keyword, string category)
        {
            return keyword.Trim().ToLowerInvariant() + "|" + category.Trim().ToLowerInvariant();
        }
    }
}
