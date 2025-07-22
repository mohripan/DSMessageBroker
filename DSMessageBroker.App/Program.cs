using DSMessageBroker.Broker;
using DSMessageBroker.Networking;
using MessageBroker.Storage;
using System.Net.Sockets;
using System.Text;

Console.WriteLine("Select role: [broker | producer | consumer]");
var role = Console.ReadLine()?.Trim().ToLowerInvariant();

switch (role)
{
    case "broker":
        await RunBrokerAsync();
        break;

    case "producer":
        await RunProducerAsync();
        break;

    case "consumer":
        await RunConsumerAsync();
        break;

    default:
        Console.WriteLine("Invalid role. Use 'broker', 'producer', or 'consumer'.");
        break;
}

async Task RunBrokerAsync()
{
    var logDir = Path.Combine(AppContext.BaseDirectory, "wal");
    var broker = new BrokerServer(logDir);
    var server = new TcpServer(5000, broker);

    var cts = new CancellationTokenSource();
    Console.CancelKeyPress += (_, e) =>
    {
        Console.WriteLine("Shutting down broker...");
        e.Cancel = true;
        cts.Cancel();
    };

    await server.StartAsync(cts.Token);
}

async Task RunProducerAsync()
{
    Console.Write("Enter topic name to produce to: ");
    var topic = Console.ReadLine() ?? "default";

    using var client = new TcpClient();
    await client.ConnectAsync("localhost", 5000);
    Console.WriteLine($"[Producer] Connected to broker on topic '{topic}'");

    using var stream = client.GetStream();
    using var writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true };
    using var reader = new StreamReader(stream, Encoding.UTF8);

    var random = new Random();

    while (true)
    {
        var payload = $"Message-{random.Next(1000, 9999)}";
        var command = $"PRODUCE|{topic}|{payload}";

        await writer.WriteLineAsync(command);
        var response = await reader.ReadLineAsync();
        Console.WriteLine($"[Producer] Sent to '{topic}': {payload} → {response}");

        await Task.Delay(1000);
    }
}

async Task RunConsumerAsync()
{
    Console.Write("Enter topic name to subscribe: ");
    var topic = Console.ReadLine() ?? "default";

    using var client = new TcpClient();
    await client.ConnectAsync("localhost", 5000);
    Console.WriteLine($"[Consumer] Connected to broker, subscribing to '{topic}'...");

    using var stream = client.GetStream();
    using var writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true };
    using var reader = new StreamReader(stream, Encoding.UTF8);

    await writer.WriteLineAsync($"SUBSCRIBE|{topic}");
    var subAck = await reader.ReadLineAsync();
    Console.WriteLine($"[Consumer] {subAck}");

    while (true)
    {
        await writer.WriteLineAsync("CONSUME");
        var response = await reader.ReadLineAsync();
        Console.WriteLine($"[Consumer] Received: {response}");

        await Task.Delay(1500);
    }
}