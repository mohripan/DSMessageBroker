using DSMessageBroker.Services;
using MessageBroker.Storage;

namespace DSMessageBroker.Broker
{
    public class TopicQueue
    {
        public string Topic { get; }
        public InMemoryQueue Queue { get; }
        public IStorageEngine Storage { get; }
        public AcknowledgedStore AckStore { get; set; }

        public TopicQueue(string topic, IStorageEngine storage, AcknowledgedStore ackStore)
        {
            Topic = topic;
            Queue = new InMemoryQueue();
            Storage = storage;
            AckStore = ackStore;
        }
    }
}
