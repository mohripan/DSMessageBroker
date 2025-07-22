namespace DSMessageBroker.Services
{
    public class ConsumerDeduplicator
    {
        private readonly HashSet<Guid> _processed = new();

        public bool IsDuplicate(Guid messageId)
        {
            lock (_processed)
            {
                return _processed.Contains(messageId);
            }
        }

        public void MarkProcessed(Guid messageId)
        {
            lock (_processed)
            {
                _processed.Add(messageId);
            }
        }
    }
}
