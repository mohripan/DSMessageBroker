using DSMessageBroker.Broker;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace DSMessageBroker.Networking
{
    public class TcpServer
    {
        private readonly TcpListener _listener;
        private readonly BrokerServer _broker;

        public TcpServer(int port, BrokerServer broker)
        {
            _listener = new TcpListener(IPAddress.Any, port);
            _broker = broker;
        }

        public async Task StartAsync(CancellationToken cancellationToken = default)
        {
            _listener.Start();
            Console.WriteLine("[Server] Listening on port 5000...");

            while (!cancellationToken.IsCancellationRequested)
            {
                var client = await _listener.AcceptTcpClientAsync(cancellationToken);
                _ = HandleClientAsync(client, cancellationToken);
            }
        }

        private async Task HandleClientAsync(TcpClient client, CancellationToken cancellationToken)
        {
            using var stream = client.GetStream();
            using var reader = new StreamReader(stream, Encoding.UTF8);
            using var writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true };

            Console.WriteLine($"[Server] Client connected.");

            string? subscribedTopic = null;

            while (!cancellationToken.IsCancellationRequested)
            {
                var line = await reader.ReadLineAsync();
                if (line == null) break;

                if (line.StartsWith("PRODUCE|"))
                {
                    var parts = line.Split('|', 3);
                    if (parts.Length != 3)
                    {
                        await writer.WriteLineAsync("ERR|Invalid PRODUCE format");
                        continue;
                    }

                    var topic = parts[1];
                    var payload = parts[2];

                    await _broker.ReceiveMessageAsync(topic, payload);
                    await writer.WriteLineAsync("ACK");
                }
                else if (line.StartsWith("SUBSCRIBE|"))
                {
                    var parts = line.Split('|', 2);
                    if (parts.Length != 2)
                    {
                        await writer.WriteLineAsync("ERR|Invalid SUBCRIBE format");
                        continue;
                    }

                    subscribedTopic = parts[1];
                    await writer.WriteLineAsync($"SUBSCRIBED|{subscribedTopic}");
                }
                else if (line == "CONSUME")
                {
                    if (subscribedTopic == null)
                    {
                        await writer.WriteLineAsync("ERR|Not subscribed to any topic");
                        continue;
                    }

                    var msg = _broker.DeliverMessage(subscribedTopic);
                    await writer.WriteLineAsync(msg?.ToString() ?? "NO_MESSAGE");
                }
                else
                {
                    await writer.WriteLineAsync("ERR|Invalid Command");
                }
            }

            Console.WriteLine("[Server] Client disconnected.");
        }
    }
}
