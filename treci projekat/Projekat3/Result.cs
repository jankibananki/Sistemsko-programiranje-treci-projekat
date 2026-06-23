using Akka.Actor;

namespace Projekat3
{
    // Observer koji prima događaje iz Rx toka i prosleđuje ih Akka.NET aktoru.
    class Result : IObserver<NewsItem>
    {
        private readonly IActorRef _newsActor;
        private readonly TaskCompletionSource<bool> _created;
        private readonly List<Task<ArticleAdded>> _pendingAdds;

        public Task Created => _created.Task;

        public Result(IActorRef newsActor)
        {
            _newsActor = newsActor;
            _created = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            _pendingAdds = new List<Task<ArticleAdded>>();
        }

        public async void OnCompleted()
        {
            try
            {
                ArticleAdded[] results = await Task.WhenAll(_pendingAdds);
                if (results.Any(result => !result.Ok))
                    throw new InvalidOperationException("Aktor nije prihvatio sve članke.");

                _created.TrySetResult(true);
            }
            catch (Exception error)
            {
                _created.TrySetException(error);
            }
        }

        public void OnError(Exception error)
        {
            _created.TrySetException(error);
        }

        public void OnNext(NewsItem value)
        {
            _pendingAdds.Add(_newsActor.Ask<ArticleAdded>(
                new AddArticle(value.RequestId, value.Title, value.Source),
                TimeSpan.FromSeconds(5)));
        }
    }
}
