using Akka.Actor;

namespace Projekat3
{
    // Observer koji prima događaje iz Rx toka i prosleđuje ih Akka.NET aktoru.
    class Result : IObserver<NewsBatch>
    {
        private readonly IActorRef _newsActor;

        public Result(IActorRef newsActor)
        {
            _newsActor = newsActor;
        }

        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
            Console.WriteLine("Rx observer error: " + error.Message);
        }

        public void OnNext(NewsBatch value)
        {
            _newsActor.Tell(new ArticlesUpdated(value.Keyword, value.Category, value.Articles));
        }
    }
}
