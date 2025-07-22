using DSMessageBroker.Broker;
using DSMessageBroker.Services;

namespace DSMessageBroker.Tests
{
    public class DeliveryTrackerTests
    {
        [Fact]
        public async Task Ack_Removes_Message()
        {
            var msg = new Message("test-topic", "payload");
            var wasRedelivered = false;

            var tracker = new DeliveryTracker(
                TimeSpan.FromSeconds(1),
                _ => wasRedelivered = true
            );

            tracker.MarkDelivered(msg);
            tracker.Acknowledge(msg.Id);

            await Task.Delay(1500);

            Assert.False(wasRedelivered);
        }

        [Fact]
        public async Task Nack_Triggers_Immediate_Redelivery()
        {
            var msg = new Message("test-topic", "payload");
            var redelivered = false;

            var tracker = new DeliveryTracker(
                TimeSpan.FromSeconds(10),
                _ => redelivered = true
            );

            tracker.MarkDelivered(msg);
            tracker.Nacknowledge(msg.Id);

            await Task.Delay(100);

            Assert.True(redelivered);
        }

        [Fact]
        public async Task Timeout_Triggers_Redelivery()
        {
            var msg = new Message("test-topic", "payload");
            var redelivered = false;

            var tracker = new DeliveryTracker(
                TimeSpan.FromMilliseconds(500),
                _ => redelivered = true
            );

            tracker.MarkDelivered(msg);

            await Task.Delay(1500);

            Assert.True(redelivered);
        }
    }
}
