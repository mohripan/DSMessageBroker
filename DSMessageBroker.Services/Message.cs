namespace DSMessageBroker.Services
{
    public class Message
    {
        public Guid Id { get; }
        public string Payload { get; }
        public DateTime Timestamp { get; }
        public string Topic { get; }

        public Message(string topic, string payload)
        {
            Id = Guid.NewGuid();
            Topic = topic;
            Payload = payload;
            Timestamp = DateTime.UtcNow;
        }

        public Message(string topic, string payload, Guid id, DateTime timestamp)
        {
            Id = id;
            Topic = topic;
            Payload = payload;
            Timestamp = timestamp;
        }

        public override string ToString() => $"[{Timestamp:HH:mm:ss}] ({Topic}) {Id} - {Payload}";
    }
}
