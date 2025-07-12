using DSMessageBroker.Services;

namespace DSMessageBroker.Broker
{
    public class BrokerServer
    {
        private readonly InMemoryQueue _queue = new();

        public void ReceiveMessage(string payload)
        {
            var msg = new Message(payload);
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
