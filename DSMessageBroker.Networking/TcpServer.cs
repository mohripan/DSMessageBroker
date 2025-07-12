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
            using var writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true};

            Console.WriteLine($"[Server] Client connected.");

            while (!cancellationToken.IsCancellationRequested)
            {
                var line = await reader.ReadLineAsync();
                if (line == null) break;

                if (line.StartsWith("PRODUCE|"))
                {
                    var payload = line.Substring(8);
                    _broker.ReceiveMessage(payload);
                    await writer.WriteLineAsync("ACK");
                }
                else if (line == "CONSUME")
                {
                    var msg = _broker.DeliverMessage();
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
