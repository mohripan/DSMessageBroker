using DSMessageBroker.Services;
using System.Collections.Concurrent;

namespace DSMessageBroker.Broker
{
    public class DeliveryTracker
    {
        private readonly ConcurrentDictionary<Guid, (Message Message, DateTime DeliveryTime)> _inFlight = new();
        private readonly TimeSpan _ackTimeout;
        private readonly Action<Message> _redeliverCallback;

        public DeliveryTracker(TimeSpan ackTimeout, Action<Message> redeliverCallback)
        {
            _ackTimeout = ackTimeout;
            _redeliverCallback = redeliverCallback;

            Task.Run(MonitorAsync);
        }

        public void MarkDelivered(Message message)
        {
            _inFlight[message.Id] = (message, DateTime.UtcNow);
        }

        public void Acknowledge(Guid messageId)
        {
            _inFlight.TryRemove(messageId, out _);
        }

        public void Nacknowledge(Guid messageId)
        {
            if (_inFlight.TryRemove(messageId, out var data))
            {
                _redeliverCallback(data.Message);
            }
        }

        private async Task MonitorAsync()
        {
            while (true)
            {
                var now = DateTime.UtcNow;
                foreach (var (id, (msg, time)) in _inFlight)
                {
                    if ((now - time) > _ackTimeout)
                    {
                        if (_inFlight.TryRemove(id, out _))
                        {
                            Console.WriteLine($"[Broker] Redelivering timed out message: {msg.Id}");
                            _redeliverCallback(msg);
                        }
                    }
                }

                await Task.Delay(1000);
            }
        }
    }
}
