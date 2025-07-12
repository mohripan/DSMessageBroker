using System.Collections.Concurrent;

namespace DSMessageBroker.Services
{
    public class InMemoryQueue
    {
        private readonly ConcurrentQueue<Message> _queue = new();
        private int _messageCount = 0;

        public void Enqueue(Message message)
        {
            _queue.Enqueue(message);
            Interlocked.Increment(ref _messageCount);
        }

        public bool TryDequeue(out Message? message)
        {
            var success = _queue.TryDequeue(out message);
            if (success)
            {
                Interlocked.Decrement(ref _messageCount);
            }
            return success;
        }

        public int Count => _messageCount;
    }
}
