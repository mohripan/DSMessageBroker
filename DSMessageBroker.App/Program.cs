using DSMessageBroker.Broker;
using DSMessageBroker.Networking;
using DSMessageBroker.Services;
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
    using var client = new TcpClient();
    await client.ConnectAsync("localhost", 5000);
    Console.WriteLine("[Consumer] Connected to broker");

    using var stream = client.GetStream();
    using var writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true };
    using var reader = new StreamReader(stream, Encoding.UTF8);

    Console.Write("Enter topic to subscribe to: ");
    var topic = Console.ReadLine();

    await writer.WriteLineAsync($"SUBSCRIBE|{topic}");
    var subResp = await reader.ReadLineAsync();
    Console.WriteLine($"[Consumer] {subResp}");

    var deduplicator = new ConsumerDeduplicator();

    while (true)
    {
        await writer.WriteLineAsync("CONSUME");
        var response = await reader.ReadLineAsync();

        if (string.IsNullOrWhiteSpace(response) || response == "NO_MESSAGE")
        {
            Console.WriteLine("[Consumer] No message available.");
            await Task.Delay(1000);
            continue;
        }

        Console.WriteLine($"[Consumer] Received: {response}");

        var parts = response.Split(' ');
        if (parts.Length < 3 || !Guid.TryParse(parts[2], out var messageId))
        {
            Console.WriteLine("[Consumer] Invalid message format");
            continue;
        }

        if (deduplicator.IsDuplicate(messageId))
        {
            Console.WriteLine($"[Consumer] Duplicate message detected, skipping: {messageId}");
            await writer.WriteLineAsync($"ACK|{messageId}");
            await reader.ReadLineAsync();
            continue;
        }

        deduplicator.MarkProcessed(messageId);

        var random = new Random();
        await Task.Delay(random.Next(500, 1500));

        if (random.NextDouble() < 0.85)
        {
            await writer.WriteLineAsync($"ACK|{messageId}");
            var ackResp = await reader.ReadLineAsync();
            Console.WriteLine($"[Consumer] Sent ACK: {ackResp}");
        }
        else
        {
            await writer.WriteLineAsync($"NACK|{messageId}");
            var nackResp = await reader.ReadLineAsync();
            Console.WriteLine($"[Consumer] Sent NACK: {nackResp}");
        }
    }
}