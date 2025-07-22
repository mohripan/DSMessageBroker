using DSMessageBroker.Services;
using MessageBroker.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DSMessageBroker.Broker
{
    public class TopicQueue
    {
        public string Topic { get; }
        public InMemoryQueue Queue { get; }
        public IStorageEngine Storage { get; }

        public TopicQueue(string topic, IStorageEngine storage)
        {
            Topic = topic;
            Queue = new InMemoryQueue();
            Storage = storage;
        }
    }
}
