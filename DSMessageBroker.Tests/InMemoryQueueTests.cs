using DSMessageBroker.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DSMessageBroker.Tests
{
    public class InMemoryQueueTests
    {
        [Fact]
        public void Enqueue_And_Dequeue_Works()
        {
            var queue = new InMemoryQueue();
            var message = new Message("test");

            queue.Enqueue(message);

            Assert.True(queue.TryDequeue(out var result));
            Assert.Equal(message.Id, result?.Id);
        }
    }
}
