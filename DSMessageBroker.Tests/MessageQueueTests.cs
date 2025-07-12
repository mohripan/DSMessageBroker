using DSMessageBroker.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DSMessageBroker.Tests
{
    public class MessageQueueTests
    {
        [Fact]
        public async Task EnqueueDequeue_ShouldWorkInOrder()
        {
            var queue = new MessageQueue();
            var msg1 = new Message { Id = Guid.NewGuid(), Body = "First", TimeStamp = DateTime.UtcNow };
            var msg2 = new Message { Id = Guid.NewGuid(), Body = "Second", TimeStamp = DateTime.UtcNow };

            await queue.EnqueueAsync(msg1);
            await queue.EnqueueAsync(msg2);

            var received1 = await queue.DequeueAsync();
            var received2 = await queue.DequeueAsync();

            Assert.Equal(msg1.Body, received1.Body);
            Assert.Equal(msg2.Body, received2.Body);
        }

        [Fact]
        public async Task Dequeue_Waits_WhenEmpty()
        {
            var queue = new MessageQueue();

            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
            var dequeueTask = queue.DequeueAsync(cts.Token);

            await Assert.ThrowsAsync<OperationCanceledException>(() => dequeueTask);
        }
    }
}
