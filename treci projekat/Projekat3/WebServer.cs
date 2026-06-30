using Akka.Actor;
using Akka.Configuration;
using System.Net;
using System.Text;

namespace Projekat3
{
    class WebServer
    {
        private readonly string _urlServer;
        private readonly object _lockConsole = new object();
        private readonly string _logFile = "server.log";
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private readonly ActorSystem _actorSystem;
        private readonly IActorRef _newsActor;
        private readonly Article _article;
        private readonly IDisposable _rxSubscription;
        private HttpListener? _listener;
        private int _requestCount = 0;

        public WebServer(string urlServer)
        {
            _urlServer = urlServer;

            Config config = ConfigurationFactory.ParseString(@"
akka.actor.default-dispatcher.throughput = 10

news-dispatcher {
    type = Dispatcher
    executor = thread-pool-executor
    throughput = 10

    thread-pool-executor {
        fixed-pool-size = 4
    }
}
");

            _actorSystem = ActorSystem.Create("NewsSystem", config);
            _newsActor = _actorSystem.ActorOf(Props.Create(() => new NewsActor(_lockConsole)).WithDispatcher("news-dispatcher"), "newsActor");
            _article = new Article(_newsActor, _lockConsole);
            _rxSubscription = _article.Subscribe(new Result(_newsActor));
            File.WriteAllText(_logFile, "Server log started: " + DateTime.Now + Environment.NewLine);
        }

        public async Task Run()
        {
            _listener = new HttpListener();
            _listener.Prefixes.Add(_urlServer);
            _listener.Start();

            Log("WebServer started on " + _urlServer);

            string apiKey = Environment.GetEnvironmentVariable("NEWS_API_KEY") ?? "";
            if (string.IsNullOrWhiteSpace(apiKey))
                Log("NEWS_API_KEY nije podešen. Rx tok je pokrenut tek kada postoji API ključ u environment promenljivoj.");
            else
                _article.Start(apiKey, TimeSpan.FromSeconds(30));

            _ = Task.Run(async () =>
            {
                while (!_cts.IsCancellationRequested && _listener.IsListening)
                {
                    try
                    {
                        HttpListenerContext context = await _listener.GetContextAsync();
                        _ = ProcessRequestAsync(context, Interlocked.Increment(ref _requestCount));
                    }
                    catch
                    {
                        break;
                    }
                }
            });
        }

        public async Task Stop()
        {
            _cts.Cancel();
            _listener?.Stop();
            _listener?.Close();
            _rxSubscription.Dispose();
            _article.Dispose();
            await _actorSystem.Terminate();
            Log("WebServer stopped.");
        }

        private async Task ProcessRequestAsync(HttpListenerContext context, int requestNumber)
        {
            HttpListenerRequest request = context.Request;
            HttpListenerResponse response = context.Response;

            try
            {
                LogRequest(request, requestNumber);

                string validation = ValidateRequest(context);
                if (!validation.Equals("OK"))
                {
                    await SendResponse(response, 400, validation);
                    Log("Request " + requestNumber + " failed: " + validation);
                    return;
                }

                string keyword = request.QueryString["keyword"] ?? "";
                string category = request.QueryString["category"] ?? "";

                if (string.IsNullOrWhiteSpace(keyword) || string.IsNullOrWhiteSpace(category))
                {
                    await SendResponse(response, 200, GetHelpText());
                    Log("Request " + requestNumber + ": help page returned.");
                    return;
                }

                await _newsActor.Ask<QueryReady>(new EnsureQuery(keyword, category), TimeSpan.FromSeconds(5));
                NewsResult finalResult = await _newsActor.Ask<NewsResult>(new GetResult(keyword, category), TimeSpan.FromSeconds(10));
                await SendResponse(response, 200, FormatResult(finalResult));
                Log("Request " + requestNumber + " successfully processed.");
            }
            catch (Exception e)
            {
                Log("Request " + requestNumber + " error: " + e.Message);
                await SendResponse(response, 500, "Greška: " + e.Message);
            }
        }

        private string ValidateRequest(HttpListenerContext context)
        {
            if (!context.Request.HttpMethod.Equals("GET", StringComparison.OrdinalIgnoreCase))
                return "METHOD IS NOT GET";

            return "OK";
        }

        private string FormatResult(NewsResult result)
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("NEWS API TOP HEADLINES");
            sb.AppendLine("Keyword: " + result.Keyword);
            sb.AppendLine("Category: " + result.Category);
            sb.AppendLine("SharpEntropy: " + result.SharpEntropyStatus);
            sb.AppendLine();

            sb.AppendLine("NASLOVI PO IZVORU:");
            foreach (var source in result.TitlesBySource.OrderBy(pair => pair.Key))
            {
                sb.AppendLine();
                sb.AppendLine(source.Key + ":");
                foreach (string title in source.Value)
                    sb.AppendLine(" - " + title);
            }

            sb.AppendLine();
            sb.AppendLine("TOPIC MODELING:");
            if (result.Topics.Count == 0)
            {
                sb.AppendLine("Nema dovoljno naslova za izdvajanje tema.");
            }
            else
            {
                foreach (Topic topic in result.Topics)
                    sb.AppendLine(" - " + topic.Name + " | keywords: " + string.Join(", ", topic.Keywords) + " | score: " + topic.Score);
            }

            return sb.ToString();
        }

        private string GetHelpText()
        {
            return "Primer poziva:\n" +
                   "http://localhost:5000/?keyword=ai&category=technology\n\n" +
                   "NEWS_API_KEY se podešava kroz environment promenljivu.\n\n" +
                   "Dozvoljene News API kategorije: business, entertainment, general, health, science, sports, technology.";
        }

        private async Task SendResponse(HttpListenerResponse response, int statusCode, string text)
        {
            byte[] buffer = Encoding.UTF8.GetBytes(text);
            response.StatusCode = statusCode;
            response.ContentType = "text/plain; charset=utf-8";
            response.ContentLength64 = buffer.Length;
            await response.OutputStream.WriteAsync(buffer, 0, buffer.Length);
            response.OutputStream.Close();
        }

        private void LogRequest(HttpListenerRequest request, int requestId)
        {
            Log("----------------------------------------------------");
            Log("Request number: " + requestId);
            Log("Request URL: " + request.Url);
            Log("Request HTTP method: " + request.HttpMethod);
            Log("Request User-agent: " + request.UserAgent);
            Log("Request Content-type: " + request.ContentType);
            Log("Request Content-length: " + request.ContentLength64);
            Log("----------------------------------------------------");
        }

        private void Log(string message)
        {
            string line = "[" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff") + "] " + message;
            lock (_lockConsole)
            {
                Console.WriteLine(line);
                File.AppendAllText(_logFile, line + Environment.NewLine);
            }
        }
    }
}
