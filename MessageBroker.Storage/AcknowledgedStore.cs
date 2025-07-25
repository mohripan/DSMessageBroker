using System.Collections.Concurrent;

namespace MessageBroker.Storage
{
    public class AcknowledgedStore
    {
        private readonly string _ackFilePath;
        private readonly ConcurrentDictionary<Guid, byte> _ackedMessages = new();
        private readonly SemaphoreSlim _fileLock = new(1, 1);

        public AcknowledgedStore(string directory, string topic)
        {
            Directory.CreateDirectory(directory);
            _ackFilePath = Path.Combine(directory, $"{topic}.acks");

            if (File.Exists(_ackFilePath))
            {
                var lines = File.ReadAllLines(_ackFilePath);
                foreach (var line in lines)
                {
                    if (Guid.TryParse(line, out var id))
                        _ackedMessages.TryAdd(id, 0);
                }
            }
        }

        public bool IsAcked(Guid id) => _ackedMessages.ContainsKey(id);

        public async Task MarkAckedAsync(Guid id)
        {
            if (_ackedMessages.TryAdd(id, 0))
            {
                await _fileLock.WaitAsync();
                try
                {
                    await File.AppendAllTextAsync(_ackFilePath, id + Environment.NewLine);
                }
                finally
                {
                    _fileLock.Release();
                }
            }
        }
    }
}
