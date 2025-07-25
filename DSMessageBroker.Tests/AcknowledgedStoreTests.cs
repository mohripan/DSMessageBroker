using DSMessageBroker.Broker;
using DSMessageBroker.Services;
using MessageBroker.Storage;

namespace DSMessageBroker.Tests
{
    public class AcknowledgedStoreTests
    {
        private string CreateTempDirectory()
        {
            var path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            Directory.CreateDirectory(path);
            return path;
        }

        [Fact]
        public async Task Can_Save_And_Load_Acked_Message()
        {
            var dir = CreateTempDirectory();
            var topic = "orders";
            var store = new AcknowledgedStore(dir, topic);

            var messageId = Guid.NewGuid();
            Assert.False(store.IsAcked(messageId));

            await store.MarkAckedAsync(messageId);

            var store2 = new AcknowledgedStore(dir, topic);
            Assert.True(store2.IsAcked(messageId));
        }

        [Fact]
        public async Task Duplicate_Acks_Do_Not_Throw()
        {
            var dir = CreateTempDirectory();
            var topic = "dupes";
            var store = new AcknowledgedStore(dir, topic);

            var messageId = Guid.NewGuid();

            await store.MarkAckedAsync(messageId);
            await store.MarkAckedAsync(messageId); // again

            var store2 = new AcknowledgedStore(dir, topic);
            Assert.True(store2.IsAcked(messageId));
        }

        [Fact]
        public async Task Concurrent_Acks_Are_Safe()
        {
            var dir = CreateTempDirectory();
            var topic = "bulk";
            var store = new AcknowledgedStore(dir, topic);

            var ids = Enumerable.Range(0, 100).Select(_ => Guid.NewGuid()).ToList();

            await Task.WhenAll(ids.Select(id => store.MarkAckedAsync(id)));

            var reloaded = new AcknowledgedStore(dir, topic);

            foreach (var id in ids)
            {
                Assert.True(reloaded.IsAcked(id));
            }
        }

        [Fact]
        public async Task TopicQueue_Does_Not_Enqueue_Acked_Messages_On_Recovery()
        {
            var dir = CreateTempDirectory();
            var topic = "skip-acked";
            var wal = new WriteAheadLog(dir, topic);
            var ackStore = new AcknowledgedStore(dir, topic);

            var msg1 = new Message(topic, "alpha");
            var msg2 = new Message(topic, "beta");

            await wal.AppendAsync(msg1);
            await wal.AppendAsync(msg2);
            await ackStore.MarkAckedAsync(msg1.Id);

            var queue = new TopicQueue(topic, wal, ackStore);
            var recovered = await wal.RecoverAsync();

            foreach (var msg in recovered)
            {
                if (!ackStore.IsAcked(msg.Id))
                    queue.Queue.Enqueue(msg);
            }

            Assert.Equal(1, queue.Queue.Count);
            queue.Queue.TryDequeue(out var remaining);
            Assert.Equal(msg2.Id, remaining?.Id);
        }
    }
}
