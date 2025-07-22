using DSMessageBroker.Services;
using MessageBroker.Storage;
using System.Text;

namespace DSMessageBroker.Tests
{
    public class WriteAheadLogTests
    {
        private string CreateTempDirectory()
        {
            var path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            Directory.CreateDirectory(path);
            return path;
        }

        [Fact]
        public async Task Append_And_Recover_Works()
        {
            var tempDir = CreateTempDirectory();
            var topic = "test-topic";
            var wal = new WriteAheadLog(tempDir, topic);

            var message1 = new Message(topic, "hello");
            var message2 = new Message(topic, "world");

            await wal.AppendAsync(message1);
            await wal.AppendAsync(message2);

            var recovered = (await wal.RecoverAsync()).ToList();

            Assert.Equal(2, recovered.Count);
            Assert.Contains(recovered, m => m.Payload == "hello");
            Assert.Contains(recovered, m => m.Payload == "world");
            Assert.All(recovered, m => Assert.Equal(topic, m.Topic));
        }

        [Fact]
        public async Task Recover_Returns_Empty_When_No_Log()
        {
            var tempDir = CreateTempDirectory();
            var wal = new WriteAheadLog(tempDir, "test-topic");

            var recovered = await wal.RecoverAsync();

            Assert.Empty(recovered);
        }

        [Fact]
        public async Task Recovered_Messages_Match_Original_Content()
        {
            var tempDir = CreateTempDirectory();
            var topic = "audit";
            var wal = new WriteAheadLog(tempDir, topic);

            var original = new Message(topic, "test-content");
            await wal.AppendAsync(original);

            var recovered = (await wal.RecoverAsync()).First();

            Assert.Equal(original.Id, recovered.Id);
            Assert.Equal(original.Payload, recovered.Payload);
            Assert.Equal(original.Timestamp.ToString("o"), recovered.Timestamp.ToString("o"));
            Assert.Equal(topic, recovered.Topic);
        }

        [Fact]
        public async Task Concurrent_Appends_Are_Safe()
        {
            var tempDir = CreateTempDirectory();
            var topic = "bulk";
            var wal = new WriteAheadLog(tempDir, topic);

            var messages = Enumerable.Range(0, 100).Select(i => new Message(topic, $"msg-{i}")).ToList();

            await Task.WhenAll(messages.Select(m => wal.AppendAsync(m)));

            var recovered = (await wal.RecoverAsync()).ToList();

            Assert.Equal(100, recovered.Count);
            foreach (var msg in messages)
            {
                Assert.Contains(recovered, r => r.Payload == msg.Payload && r.Id == msg.Id && r.Topic == topic);
            }
        }

        [Fact]
        public async Task Recover_Ignores_Invalid_Lines()
        {
            var tempDir = CreateTempDirectory();
            var topic = "faulty";
            var filePath = Path.Combine(tempDir, $"{topic}.log");

            var valid = new Message(topic, "valid");
            var invalidLine = "not|a|valid|message";

            var content = new StringBuilder();
            content.AppendLine($"{valid.Id}|{valid.Timestamp:o}|{valid.Payload}");
            content.AppendLine(invalidLine);

            await File.WriteAllTextAsync(filePath, content.ToString());

            var wal = new WriteAheadLog(tempDir, topic);
            var recovered = (await wal.RecoverAsync()).ToList();

            Assert.Single(recovered);
            Assert.Equal("valid", recovered[0].Payload);
            Assert.Equal(topic, recovered[0].Topic);
        }
    }
}
