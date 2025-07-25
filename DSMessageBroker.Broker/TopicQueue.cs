using DSMessageBroker.Services;
using MessageBroker.Storage;
using System.Threading.Channels;

namespace DSMessageBroker.Broker
{
    public class TopicQueue
    {
        public string Topic { get; }
        public Channel<Message> Channel { get; }
        public IStorageEngine Storage { get; }
        public AcknowledgedStore AckStore { get; set; }

        public TopicQueue(string topic, IStorageEngine storage, AcknowledgedStore ackStore)
        {
            Topic = topic;
            Channel = System.Threading.Channels
                .Channel.CreateUnbounded<Message>(new UnboundedChannelOptions
            {
                SingleReader = false,
                SingleWriter = false
            });
            Storage = storage;
            AckStore = ackStore;
        }
    }
}
