using DSMessageBroker.Services;

namespace MessageBroker.Storage
{
    public class WriteAheadLog : IStorageEngine
    {
        private readonly string _logDirectory;
        private readonly string _logFilePath;
        private readonly SemaphoreSlim _lock = new(1, 1);

        public WriteAheadLog(string logDirectory)
        {
            _logDirectory = logDirectory;
            _logFilePath = Path.Combine(logDirectory, "wal.log");

            Directory.CreateDirectory(_logDirectory);
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
                var parts = line.Split('|', 3);
                if (parts.Length != 3) continue;

                var id = Guid.Parse(parts[0]);
                var timestamp = DateTime.Parse(parts[1]);
                var payload = parts[2];

                var message = new Message(payload, id, timestamp);
                messages.Add(message);
            }

            return messages;
        }
    }
}
