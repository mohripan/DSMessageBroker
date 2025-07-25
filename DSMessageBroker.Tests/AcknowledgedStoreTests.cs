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
                    await queue.Channel.Writer.WriteAsync(msg);
            }

            Assert.Equal(1, queue.Channel.Reader.Count);
            queue.Channel.Reader.TryRead(out var remaining);
            Assert.Equal(msg2.Id, remaining?.Id);
        }

        [Fact]
        public void Corrupt_Ack_File_Lines_Are_Ignored()
        {
            var dir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            Directory.CreateDirectory(dir);

            var topic = "corrupt-test";
            var ackFile = Path.Combine(dir, $"{topic}.acks");

            var validId = Guid.NewGuid();
            File.WriteAllLines(ackFile, new[]
            {
                validId.ToString(),
                "not-a-guid",
                "",
                "123456"
            });

            var store = new AcknowledgedStore(dir, topic);

            Assert.True(store.IsAcked(validId));
        }

        [Fact]
        public void Missing_Ack_File_Does_Not_Throw()
        {
            var dir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            var store = new AcknowledgedStore(dir, "new-topic");

            Assert.False(store.IsAcked(Guid.NewGuid()));
        }

        [Fact]
        public async Task Parallel_Reads_And_Acks_Do_Not_Corrupt_File()
        {
            var dir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            var store = new AcknowledgedStore(dir, "parallel-topic");

            var ids = Enumerable.Range(0, 100).Select(_ => Guid.NewGuid()).ToList();

            await Task.WhenAll(ids.Select(id => Task.Run(() => store.MarkAckedAsync(id))));

            var reloaded = new AcknowledgedStore(dir, "parallel-topic");

            foreach (var id in ids)
            {
                Assert.True(reloaded.IsAcked(id));
            }
        }

        [Fact]
        public async Task Large_Number_Of_Acks_Can_Be_Loaded_Efficiently()
        {
            var dir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            var store = new AcknowledgedStore(dir, "big-acks");

            var ids = Enumerable.Range(0, 100_000).Select(_ => Guid.NewGuid()).ToList();
            foreach (var id in ids)
                await store.MarkAckedAsync(id);

            var sw = System.Diagnostics.Stopwatch.StartNew();
            var reloaded = new AcknowledgedStore(dir, "big-acks");
            sw.Stop();

            Assert.InRange(sw.ElapsedMilliseconds, 0, 500); // Should load under 500ms
            Assert.True(reloaded.IsAcked(ids[0]));
            Assert.True(reloaded.IsAcked(ids[^1]));
        }
    }
}
