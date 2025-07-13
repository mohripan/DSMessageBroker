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

        [Fact]
        public void Dequeue_EmptyQueue_ReturnsFalse()
        {
            var queue = new InMemoryQueue();

            var result = queue.TryDequeue(out var message);

            Assert.False(result);
            Assert.Null(message);
        }

        [Fact]
        public void Queue_Count_TracksCorrectly()
        {
            var queue = new InMemoryQueue();
            Assert.Equal(0, queue.Count);

            queue.Enqueue(new Message("msg1"));
            queue.Enqueue(new Message("msg2"));

            Assert.Equal(2, queue.Count);

            queue.TryDequeue(out _);
            Assert.Equal(1, queue.Count);

            queue.TryDequeue(out _);
            Assert.Equal(0, queue.Count);
        }

        [Fact]
        public void ConcurrentEnqueue_Dequeue_IsThreadSafe()
        {
            var queue = new InMemoryQueue();
            var total = 1000;

            Parallel.For(0, total, i =>
            {
                queue.Enqueue(new Message($"msg-{i}"));
            });

            var dequeued = new List<Message>();
            Parallel.For(0, total, _ =>
            {
                if (queue.TryDequeue(out var msg) && msg != null)
                {
                    lock (dequeued)
                    {
                        dequeued.Add(msg);
                    }
                }
            });

            Assert.Equal(total, dequeued.Count);
            Assert.Equal(0, queue.Count);
        }

        [Fact]
        public void Message_ToString_FormatsCorrectly()
        {
            var message = new Message("test-payload");
            var str = message.ToString();

            Assert.Contains("test-payload", str);
            Assert.Contains(message.Id.ToString(), str);
        }
    }
}
