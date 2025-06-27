using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DSMessageBroker.Services
{
    public class Message
    {
        public Guid Id { get; set; }
        public string Body { get; set; }
        public DateTime TimeStamp { get; set; }
    }
}
