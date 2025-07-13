using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DSMessageBroker.Services
{
    public class Message
    {
        public Guid Id { get; }
        public string Payload { get; }
        public DateTime Timestamp { get; }

        public Message(string payload)
        {
            Id = Guid.NewGuid();
            Payload = payload;
            Timestamp = DateTime.UtcNow;
        }

        public Message(string payload, Guid id, DateTime timestamp)
        {
            Id = id;
            Payload = payload;
            Timestamp = timestamp;
        }

        public override string ToString() => $"[{Timestamp:HH:mm:ss}] {Id} - {Payload}";
    }
}
