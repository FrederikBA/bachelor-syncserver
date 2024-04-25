using System.Text.Json;
using Confluent.Kafka;
using Shared.Integration.Models.Dtos.Sync;
using SyncServer.Services;
using Config = Shared.Integration.Configuration.Config;

namespace SyncServer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("Sync server is starting...");

            // Kafka consumer configuration
            var config = new ConsumerConfig
            {
                BootstrapServers = Config.Kafka.BootstrapServers,
                GroupId = "sync_group",
                AutoOffsetReset = AutoOffsetReset.Latest
            };
            using var c = new ConsumerBuilder<Ignore, string>(config).Build();

            // Subscribe to multiple topics
            c.Subscribe(Config.Kafka.Topics.SyncAddWs);
            c.Subscribe(Config.Kafka.Topics.SyncUpdateWs);
            c.Subscribe(Config.Kafka.Topics.SyncDeleteWs);
            c.Subscribe(Config.Kafka.Topics.SyncAddProduct);
            c.Subscribe(Config.Kafka.Topics.SyncUpdateProduct);
            c.Subscribe(Config.Kafka.Topics.SyncDeleteProduct);
            c.Subscribe(Config.Kafka.Topics.SyncAddUser);

            Console.WriteLine("Waiting for sync requests...");

            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };
            try
            {
                while (true)
                {
                    try
                    {
                        var cr = c.Consume(cts.Token);
                        // Process the received message
                        Console.WriteLine($"Received message from topic '{cr.Topic}', message: '{cr.Value}'");

                        //Initialize services
                        var syncService = new SyncService();

                        // Process the message according to its topic
                        var wsMessage = new SyncWarningSentenceDto();
                        var productWsMessage = new SyncProductWarningSentenceDto();
                        var userMessage = new SyncUserDto();

                        switch (cr.Topic)
                        {
                            case Config.Kafka.Topics.SyncAddWs:
                                wsMessage = JsonSerializer.Deserialize<SyncWarningSentenceDto>(cr.Value);
                                Console.WriteLine
                                (
                                    "Synchronizing Warning Sentence - Adding Warning Sentence with Id: " +
                                    wsMessage.WarningSentenceId
                                );

                                syncService.SyncWarningSentence(wsMessage);
                                break;
                            case Config.Kafka.Topics.SyncUpdateWs:
                                wsMessage = JsonSerializer.Deserialize<SyncWarningSentenceDto>(cr.Value);
                                Console.WriteLine
                                (
                                    "Synchronizing Warning Sentence - Updating Warning Sentence with Id: " +
                                    wsMessage.WarningSentenceId
                                );

                                syncService.SyncWarningSentence(wsMessage);
                                break;
                            case Config.Kafka.Topics.SyncDeleteWs:
                                wsMessage = JsonSerializer.Deserialize<SyncWarningSentenceDto>(cr.Value);
                                Console.WriteLine
                                (
                                    "Synchronizing Warning Sentence - Deleting Warning Sentence with Id: " +
                                    wsMessage.WarningSentenceId
                                );

                                syncService.SyncWarningSentence(wsMessage);
                                break;
                            case Config.Kafka.Topics.SyncAddProduct:
                                productWsMessage = JsonSerializer.Deserialize<SyncProductWarningSentenceDto>(cr.Value);
                                Console.WriteLine
                                (
                                    "Synchronizing Product - Adding Warning Sentence with Id: " +
                                    productWsMessage.WarningSentenceId + " to Product with Id: " +
                                    productWsMessage.ProductId
                                );

                                syncService.SyncProducts(productWsMessage);
                                break;
                            case Config.Kafka.Topics.SyncDeleteProduct:
                                productWsMessage = JsonSerializer.Deserialize<SyncProductWarningSentenceDto>(cr.Value);
                                Console.WriteLine
                                (
                                    "Synchronizing Product - Removing Warning Sentence with Id: " +
                                    productWsMessage.WarningSentenceId + " from Product with Id: " +
                                    productWsMessage.ProductId
                                );

                                syncService.SyncProducts(productWsMessage);
                                break;
                            case Config.Kafka.Topics.SyncAddUser:
                                userMessage = JsonSerializer.Deserialize<SyncUserDto>(cr.Value);
                                Console.WriteLine
                                (
                                    "Synchronizing User - Adding User with Id: " + userMessage.UserId
                                );

                                syncService.SyncUsers(userMessage);
                                break;
                        }
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Error occurred: {e.Error.Reason}");
                        Console.WriteLine($"Exception details: {e}");
                    }
                    catch (OperationCanceledException)
                    {
                        Console.WriteLine("Operation was cancelled.");
                        c.Close();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An unexpected error occurred: {ex}");
            }
        }
    }
}