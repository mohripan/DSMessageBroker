using DSMessageBroker.Services;
using MessageBroker.Storage;

namespace DSMessageBroker.Broker
{
    public class BrokerServer
    {
        private readonly InMemoryQueue _queue = new();
        private readonly IStorageEngine _storage;

        public BrokerServer(IStorageEngine storage)
        {
            _storage = storage;

            Console.WriteLine("[Broker] Recovering message from log...");
            var recovered = _storage.RecoverAsync().Result;
            foreach (var message in recovered)
            {
                _queue.Enqueue(message);
            }
            Console.WriteLine($"[Broker] Recovery complete. {_queue.Count} messages loaded.");
        }

        public async Task ReceiveMessage(string payload)
        {
            var msg = new Message(payload);
            await _storage.AppendAsync(msg);
            _queue.Enqueue(msg);

            Console.WriteLine($"[Broker] Received: {msg}");
        }

        public Message? DeliverMessage()
        {
            if (_queue.TryDequeue(out var message))
            {
                Console.WriteLine($"[Broker] Delivered: {message}");
                return message;
            }
            return null;
        }

        public int GetQueueLength() => _queue.Count;
    }
}
