using DSMessageBroker.Services;
using MessageBroker.Storage;
using System.Collections.Concurrent;

namespace DSMessageBroker.Broker
{
    public class BrokerServer
    {
        private readonly string _logDirectory;
        private readonly ConcurrentDictionary<string, TopicQueue> _topics = new();

        public BrokerServer(string logDirectory)
        {
            _logDirectory = logDirectory;

            Console.WriteLine("[Broker] Recovering topics...");

            foreach (var file in Directory.GetFiles(_logDirectory, "*.log"))
            {
                var topic = Path.GetFileNameWithoutExtension(file);
                var wal = new WriteAheadLog(_logDirectory, topic);
                var queue = new TopicQueue(topic, wal);

                var messages = wal.RecoverAsync().Result;
                foreach (var message in messages)
                {
                    queue.Queue.Enqueue(message);
                }

                _topics[topic] = queue;
                Console.WriteLine($"[Broker] Loaded topic '{topic}' with {queue.Queue.Count} messages.");
            }
        }

        public async Task ReceiveMessageAsync(string topic, string payload)
        {
            var queue = GetOrCreateTopicQueue(topic);
            var message = new Message(topic, payload);
            await queue.Storage.AppendAsync(message);
            queue.Queue.Enqueue(message);

            Console.WriteLine($"[Broker] [{topic}] Received: {message}");
        }

        public Message? DeliverMessage(string topic)
        {
            if (_topics.TryGetValue(topic, out var queue))
            {
                if (queue.Queue.TryDequeue(out var message))
                {
                    Console.WriteLine($"[Broker] [{topic}] Delivered: {message}");
                    return message;
                }
            }
            return null;
        }

        private TopicQueue GetOrCreateTopicQueue(string topic)
        {
            return _topics.GetOrAdd(topic, t =>
            {
                var wal = new WriteAheadLog(_logDirectory, topic);
                return new TopicQueue(t, wal);
            });
        }
    }
}
