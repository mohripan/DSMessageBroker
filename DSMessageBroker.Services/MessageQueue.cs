using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace DSMessageBroker.Services
{
    public class MessageQueue
    {
        private readonly Channel<Message> _channel;

        public MessageQueue(int capacity = 1000)
        {
            var options = new BoundedChannelOptions(capacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = false,
                SingleWriter = false
            };

            _channel = Channel.CreateBounded<Message>(options);
        }

        public async Task EnqueueAsync(Message message, CancellationToken cancellationToken = default)
        {
            await _channel.Writer.WriteAsync(message, cancellationToken);
        }

        public async Task<Message> DequeueAsync(CancellationToken cancellationToken = default)
        {
            return await _channel.Reader.ReadAsync(cancellationToken);
        }

        public bool TryPeek(out Message? message)
        {
            return _channel.Reader.TryPeek(out message);
        }

        public int Count => _channel.Reader.Count;
    }
}
