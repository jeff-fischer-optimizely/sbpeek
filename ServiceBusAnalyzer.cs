// ServiceBusAnalyzer.cs
// Version: 2.0.0
// CLI tool to peek into Azure Service Bus and analyze message patterns
// Usage: dotnet run -- [args]

using System;
using System.Collections.Generic;
using System.CommandLine;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Azure.Identity;
using Azure.Messaging.ServiceBus;

namespace ServiceBusAnalyzer
{
    class Program
    {
        static async Task<int> Main(string[] args)
        {
            var rootCmd = new RootCommand("Analyze Service Bus Tool v2.0.0");
            var namespaceOpt = new Option<string>("--namespace", "Azure Service Bus namespace") { IsRequired = true };
            var topicOpt = new Option<string>("--topic_name", () => "mysiteevents", "Topic name");
            var subscriptionOpt = new Option<string>("--subscription_name", "Subscription name") { IsRequired = true };
            var messageSampleOpt = new Option<int>("--message_sample", () => 256, "Number of messages to collect");
            var logLevelOpt = new Option<string>("--log_level", () => "Error", "Logging level (e.g. Debug, Info, Warning, Error)");
            var pollingOpt = new Option<int>("--polling", () => 2, "Polling interval in seconds");
            var outputOpt = new Option<string>("--output", "Write results to specified file instead of stdout.");
            var prefixLengthOpt = new Option<int?>("--prefix-length", "Optional prefix length to group extracted values by.");
            var minCountOpt = new Option<int?>("--min-count", "Optional lower threshold for count");
            var dumpOpt = new Option<string>("--dump", "Dump raw peeked messages to a JSON file for further analysis.");

            rootCmd.AddOption(namespaceOpt);
            rootCmd.AddOption(topicOpt);
            rootCmd.AddOption(subscriptionOpt);
            rootCmd.AddOption(messageSampleOpt);
            rootCmd.AddOption(logLevelOpt);
            rootCmd.AddOption(pollingOpt);
            rootCmd.AddOption(outputOpt);
            rootCmd.AddOption(prefixLengthOpt);
            rootCmd.AddOption(minCountOpt);
            rootCmd.AddOption(dumpOpt);

            rootCmd.SetHandler(async (context) =>
            {
                var parseResult = context.ParseResult;
                var namespaceName = parseResult.GetValueForOption(namespaceOpt);
                var topicName = parseResult.GetValueForOption(topicOpt);
                var subscriptionName = parseResult.GetValueForOption(subscriptionOpt);
                var messageSample = parseResult.GetValueForOption(messageSampleOpt);
                var logLevel = parseResult.GetValueForOption(logLevelOpt);
                var polling = parseResult.GetValueForOption(pollingOpt);
                var output = parseResult.GetValueForOption(outputOpt);
                var prefixLength = parseResult.GetValueForOption(prefixLengthOpt);
                var minCount = parseResult.GetValueForOption(minCountOpt);
                var dump = parseResult.GetValueForOption(dumpOpt);

                var credential = new AzureCliCredential();
                var fullyQualifiedNamespace = $"{namespaceName}.servicebus.windows.net";
                var client = new ServiceBusClient(fullyQualifiedNamespace, credential);

                Console.WriteLine("Service Bus connection established.");

                var messages = await PeekMessages(client, topicName, subscriptionName, messageSample, polling);

                if (!string.IsNullOrEmpty(dump))
                {
                    DumpMessages(messages, dump);
                    Console.WriteLine($"Raw messages dumped to {dump}");
                }

                var analyzer = new MessageAnalyzer();
                var results = messages.Select(m => analyzer.ProcessMessage(m)).ToList();
                var report = analyzer.BuildAggregateReport(results, prefixLength, minCount);
                var formatted = analyzer.FormatAggregateReport(report, topicName, subscriptionName, namespaceName);

                if (!string.IsNullOrEmpty(output))
                {
                    File.WriteAllText(output, formatted, Encoding.UTF8);
                    Console.WriteLine($"\nResults written to {output}");
                }
                else
                {
                    Console.WriteLine(formatted);
                }
            });

            return await rootCmd.InvokeAsync(args);
        }

        static async Task<List<ServiceBusReceivedMessage>> PeekMessages(ServiceBusClient client, string topic, string subscription, int messageSampleCount, int pollingInterval)
        {
            var allMessages = new List<ServiceBusReceivedMessage>();
            var seenMessageIds = new HashSet<string>();
            int removedDupes = 0;
            Console.WriteLine($"Collecting {messageSampleCount} messages from topic: {topic}, subscription: {subscription}...");
            var receiver = client.CreateReceiver(topic, subscription);
            while (allMessages.Count < messageSampleCount)
            {
                int toFetch = Math.Min(500, messageSampleCount - allMessages.Count);
                IReadOnlyList<ServiceBusReceivedMessage> batch = await receiver.PeekMessagesAsync(toFetch);
                if (batch == null || batch.Count == 0)
                {
                    await Task.Delay(pollingInterval * 1000);
                    continue;
                }
                foreach (var msg in batch)
                {
                    if (seenMessageIds.Add(msg.MessageId))
                        allMessages.Add(msg);
                    else
                        removedDupes++;
                }
            }
            Console.WriteLine($"\nPeeking completed. {allMessages.Count} messages collected. {removedDupes} duplicates removed.");
            return allMessages;
        }

        static void DumpMessages(List<ServiceBusReceivedMessage> messages, string filePath)
        {
            var dumped = messages.Select(msg =>
            {
                string bodyText;
                try { bodyText = Encoding.UTF8.GetString(msg.Body.ToArray()); }
                catch { bodyText = Convert.ToBase64String(msg.Body.ToArray()); }

                return new
                {
                    msg.MessageId,
                    msg.ContentType,
                    msg.Subject,
                    msg.CorrelationId,
                    EnqueuedTime = msg.EnqueuedTime.ToString("o"),
                    ExpiresAt = msg.ExpiresAt.ToString("o"),
                    TimeToLive = msg.TimeToLive.ToString(),
                    msg.DeliveryCount,
                    msg.SequenceNumber,
                    ApplicationProperties = msg.ApplicationProperties.ToDictionary(
                        kv => kv.Key,
                        kv => kv.Value?.ToString()),
                    Body = bodyText
                };
            });

            var json = JsonSerializer.Serialize(dumped, new JsonSerializerOptions
            {
                WriteIndented = true
            });
            File.WriteAllText(filePath, json, Encoding.UTF8);
        }
    }
}
