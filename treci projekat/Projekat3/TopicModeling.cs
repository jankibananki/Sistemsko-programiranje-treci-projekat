using SharpEntropy;
using System.Text.RegularExpressions;

namespace Projekat3
{
    class Topic
    {
        public string Name { get; set; } = "";
        public List<string> Keywords { get; set; } = new List<string>();
        public int TitleCount { get; set; }
        public double Score { get; set; }
    }

    class TopicAnalysis
    {
        public List<Topic> Topics { get; set; } = new List<Topic>();
        public string Status { get; set; } = "";
    }

    // SharpEntropy je Maximum Entropy klasifikator. Kandidati tema se izdvajaju iz
    // naslova, a MaxEnt model zatim uči veze između reči i kandidata i ocenjuje teme.
    class TopicModeling
    {
        private static readonly HashSet<string> _stopWords = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "the", "and", "for", "with", "from", "that", "this", "are", "was", "were", "has", "have",
            "new", "about", "into", "after", "over", "more", "will", "you", "your", "their", "they"
        };

        public static TopicAnalysis Run(List<string> titles)
        {
            List<string[]> contexts = titles
                .Select(title => GetWords(title).Distinct(StringComparer.OrdinalIgnoreCase).ToArray())
                .Where(words => words.Length > 0)
                .ToList();

            List<string> candidates = contexts
                .SelectMany(words => words)
                .GroupBy(word => word, StringComparer.OrdinalIgnoreCase)
                .OrderByDescending(group => group.Count())
                .ThenBy(group => group.Key)
                .Take(5)
                .Select(group => group.Key)
                .ToList();

            if (contexts.Count < 2 || candidates.Count < 2)
            {
                return new TopicAnalysis
                {
                    Topics = CreateFrequencyTopics(contexts, candidates),
                    Status = "Nema dovoljno podataka za treniranje SharpEntropy MaxEnt modela."
                };
            }

            try
            {
                List<TrainingEvent> events = new List<TrainingEvent>();
                foreach (string[] context in contexts)
                {
                    foreach (string candidate in candidates.Where(candidate => context.Contains(candidate, StringComparer.OrdinalIgnoreCase)))
                        events.Add(new TrainingEvent(candidate, context));
                }

                GisTrainer trainer = new GisTrainer();
                trainer.TrainModel(new InMemoryEventReader(events), 100, 1);
                GisModel model = new GisModel(trainer);

                Dictionary<string, List<string[]>> assignedContexts = candidates.ToDictionary(
                    candidate => candidate,
                    _ => new List<string[]>(),
                    StringComparer.OrdinalIgnoreCase);
                Dictionary<string, double> probabilitySums = candidates.ToDictionary(
                    candidate => candidate,
                    _ => 0.0,
                    StringComparer.OrdinalIgnoreCase);

                foreach (string[] context in contexts)
                {
                    double[] probabilities = model.Evaluate(context);
                    string bestTopic = model.GetBestOutcome(probabilities);
                    assignedContexts[bestTopic].Add(context);

                    foreach (string candidate in candidates)
                    {
                        int outcomeIndex = model.GetOutcomeIndex(candidate);
                        if (outcomeIndex >= 0)
                            probabilitySums[candidate] += probabilities[outcomeIndex];
                    }
                }

                List<Topic> topics = candidates
                    .Select(candidate => CreateModelTopic(
                        candidate,
                        assignedContexts[candidate],
                        probabilitySums[candidate] / contexts.Count))
                    .Where(topic => topic.TitleCount > 0)
                    .OrderByDescending(topic => topic.Score)
                    .ToList();

                return new TopicAnalysis
                {
                    Topics = topics,
                    Status = "SharpEntropy GIS MaxEnt model uspešno treniran nad " + events.Count + " događaja."
                };
            }
            catch (Exception error)
            {
                return new TopicAnalysis
                {
                    Topics = CreateFrequencyTopics(contexts, candidates),
                    Status = "SharpEntropy model nije mogao da se trenira; primenjen frekvencijski fallback: " + error.Message
                };
            }
        }

        private static Topic CreateModelTopic(string name, List<string[]> contexts, double averageProbability)
        {
            List<string> keywords = contexts
                .SelectMany(context => context)
                .GroupBy(word => word, StringComparer.OrdinalIgnoreCase)
                .OrderByDescending(group => group.Count())
                .ThenBy(group => group.Key)
                .Take(5)
                .Select(group => group.Key)
                .ToList();

            if (!keywords.Contains(name, StringComparer.OrdinalIgnoreCase))
                keywords.Insert(0, name);

            return new Topic
            {
                Name = name,
                Keywords = keywords.Take(5).ToList(),
                TitleCount = contexts.Count,
                Score = Math.Round(averageProbability, 3)
            };
        }

        private static List<Topic> CreateFrequencyTopics(List<string[]> contexts, List<string> candidates)
        {
            return candidates.Select(candidate =>
            {
                List<string[]> matching = contexts
                    .Where(context => context.Contains(candidate, StringComparer.OrdinalIgnoreCase))
                    .ToList();
                return CreateModelTopic(candidate, matching, matching.Count / Math.Max(1.0, contexts.Count));
            }).ToList();
        }

        private static IEnumerable<string> GetWords(string text)
        {
            foreach (Match match in Regex.Matches(text.ToLowerInvariant(), "[a-zA-Z]{3,}"))
            {
                string word = match.Value;
                if (!_stopWords.Contains(word))
                    yield return word;
            }
        }

        private sealed class InMemoryEventReader : ITrainingEventReader
        {
            private readonly IEnumerator<TrainingEvent> _events;
            private TrainingEvent? _next;

            public InMemoryEventReader(IEnumerable<TrainingEvent> events)
            {
                _events = events.GetEnumerator();
                MoveNext();
            }

            public bool HasNext() => _next != null;

            public TrainingEvent ReadNextEvent()
            {
                TrainingEvent current = _next ?? throw new InvalidOperationException("Nema više trening događaja.");
                MoveNext();
                return current;
            }

            private void MoveNext()
            {
                _next = _events.MoveNext() ? _events.Current : null;
            }
        }
    }
}
