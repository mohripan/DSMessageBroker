using DSMessageBroker.Services;
using System.Globalization;

namespace MessageBroker.Storage
{
    public class WriteAheadLog : IStorageEngine
    {
        private readonly string _logFilePath;
        private readonly SemaphoreSlim _lock = new(1, 1);

        public WriteAheadLog(string logDirectory, string topic)
        {
            Directory.CreateDirectory(logDirectory);
            _logFilePath = Path.Combine(logDirectory, $"{topic}.log");
        }

        public async Task AppendAsync(Message message)
        {
            var line = $"{message.Id}|{message.Timestamp:o}|{message.Payload}{Environment.NewLine}";

            await _lock.WaitAsync();
            try
            {
                await File.AppendAllTextAsync(_logFilePath, line);
            }
            finally
            {
                _lock.Release();
            }
        }

        public async Task<IEnumerable<Message>> RecoverAsync()
        {
            var messages = new List<Message>();

            if (!File.Exists(_logFilePath))
                return messages;

            var lines = await File.ReadAllLinesAsync(_logFilePath);

            foreach (var line in lines)
            {
                try
                {
                    var parts = line.Split('|', 3);
                    if (parts.Length != 3) continue;

                    var id = Guid.Parse(parts[0]);
                    var timestamp = DateTime.Parse(parts[1], null, DateTimeStyles.AdjustToUniversal);
                    var payload = parts[2];

                    var topic = Path.GetFileNameWithoutExtension(_logFilePath);

                    var message = new Message(topic, payload, id, timestamp);
                    messages.Add(message);
                }
                catch
                {
                    // Skip malformed lines
                }
            }

            return messages;
        }
    }
}
