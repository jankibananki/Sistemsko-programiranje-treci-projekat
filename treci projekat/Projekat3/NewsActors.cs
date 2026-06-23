using Akka.Actor;
namespace Projekat3
{
    // Poruke koje web server i Rx tok šalju aktorima.
    record StartRequest(long RequestId, string Keyword, string Category);
    record AddArticle(long RequestId, string Title, string Source);
    record GetResult(long RequestId);
    record CancelRequest(long RequestId);
    record RequestStarted(bool Ok);
    record ArticleAdded(bool Ok);

    class NewsResult
    {
        public long RequestId { get; set; }
        public string Keyword { get; set; } = "";
        public string Category { get; set; } = "";
        public Dictionary<string, List<string>> TitlesBySource { get; set; } = new Dictionary<string, List<string>>();
        public List<Topic> Topics { get; set; } = new List<Topic>();
        public string SharpEntropyStatus { get; set; } = "";
    }

    class NewsActor : ReceiveActor
    {
        private sealed class RequestState
        {
            public string Keyword { get; }
            public string Category { get; }
            public Dictionary<string, List<string>> TitlesBySource { get; } =
                new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
            public List<string> AllTitles { get; } = new List<string>();

            public RequestState(string keyword, string category)
            {
                Keyword = keyword;
                Category = category;
            }
        }

        private readonly object _lockConsole;
        private readonly Dictionary<long, RequestState> _requests = new Dictionary<long, RequestState>();

        public NewsActor(object lockConsole)
        {
            _lockConsole = lockConsole;

            Receive<StartRequest>(message =>
            {
                _requests[message.RequestId] = new RequestState(message.Keyword, message.Category);
                WriteToConsole("Akka actor: započeta obrada zahteva " + message.RequestId);
                Sender.Tell(new RequestStarted(true));
            });

            Receive<AddArticle>(message =>
            {
                if (!_requests.TryGetValue(message.RequestId, out RequestState? state))
                {
                    Sender.Tell(new ArticleAdded(false));
                    return;
                }

                if (!state.TitlesBySource.TryGetValue(message.Source, out List<string>? titles))
                {
                    titles = new List<string>();
                    state.TitlesBySource[message.Source] = titles;
                }

                titles.Add(message.Title);
                state.AllTitles.Add(message.Title);
                Sender.Tell(new ArticleAdded(true));
            });

            Receive<GetResult>(message =>
            {
                if (!_requests.Remove(message.RequestId, out RequestState? state))
                {
                    Sender.Tell(new Status.Failure(
                        new InvalidOperationException("Nepoznat ili završen zahtev: " + message.RequestId)));
                    return;
                }

                Dictionary<string, List<string>> copy = state.TitlesBySource.ToDictionary(
                    pair => pair.Key,
                    pair => pair.Value.Distinct().OrderBy(title => title).ToList());

                TopicAnalysis analysis = TopicModeling.Run(state.AllTitles);

                NewsResult result = new NewsResult
                {
                    RequestId = message.RequestId,
                    Keyword = state.Keyword,
                    Category = state.Category,
                    TitlesBySource = copy,
                    Topics = analysis.Topics,
                    SharpEntropyStatus = analysis.Status
                };

                WriteToConsole("Akka actor: vraćen rezultat za zahtev " + message.RequestId);
                Sender.Tell(result);
            });

            Receive<CancelRequest>(message =>
            {
                _requests.Remove(message.RequestId);
            });
        }

        private void WriteToConsole(string message)
        {
            lock (_lockConsole)
            {
                Console.WriteLine(message);
            }
        }
    }
}
