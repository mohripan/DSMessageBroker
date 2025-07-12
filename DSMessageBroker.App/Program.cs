using DSMessageBroker.Broker;
using DSMessageBroker.Networking;

var broker = new BrokerServer();
var server = new TcpServer(5000, broker);

var cts = new CancellationTokenSource();

Console.CancelKeyPress += (_, e) =>
{
    Console.WriteLine("Shutting down...");
    e.Cancel = true;
    cts.Cancel();
};

await server.StartAsync(cts.Token);