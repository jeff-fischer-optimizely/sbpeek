using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Azure.Core.Amqp;
using Azure.Messaging.ServiceBus;
using Xunit;
using Xunit.Abstractions;

namespace ServiceBusAnalyzer.Tests
{
    /// <summary>
    /// Functional tests that exercise the full analysis pipeline:
    /// message creation → processing → grouping → aggregate report → formatted output.
    /// Uses ServiceBusModelFactory to create realistic ServiceBusReceivedMessage instances.
    /// </summary>
    public class FunctionalTests
    {
        private readonly ITestOutputHelper _output;
        private readonly MessageAnalyzer _analyzer = new();

        public FunctionalTests(ITestOutputHelper output)
        {
            _output = output;
        }

        // ---------- helpers ----------

        private static ServiceBusReceivedMessage CreateMessage(
            string body,
            string contentType = null,
            string messageId = null,
            DateTimeOffset? enqueuedTime = null,
            TimeSpan? ttl = null)
        {
            messageId ??= Guid.NewGuid().ToString();
            enqueuedTime ??= DateTimeOffset.UtcNow.AddHours(-1);
            ttl ??= TimeSpan.FromDays(14);

            return ServiceBusModelFactory.ServiceBusReceivedMessage(
                body: BinaryData.FromString(body),
                messageId: messageId,
                contentType: contentType,
                enqueuedTime: enqueuedTime.Value,
                timeToLive: ttl.Value);
        }

        private static List<ServiceBusReceivedMessage> GenerateJsonBatch(int count, string[] messageValues = null)
        {
            messageValues ??= new[] { "UserCreated", "OrderPlaced", "ItemShipped", "PaymentProcessed", "InventoryUpdated" };
            var rng = new Random(42);
            var messages = new List<ServiceBusReceivedMessage>();
            for (int i = 0; i < count; i++)
            {
                var val = messageValues[rng.Next(messageValues.Length)];
                var age = TimeSpan.FromHours(rng.Next(1, 72));
                var ttl = TimeSpan.FromDays(rng.Next(7, 30));
                var body = $"{{\"message\": \"{val}\", \"id\": {i}}}";
                messages.Add(CreateMessage(body, "application/json",
                    enqueuedTime: DateTimeOffset.UtcNow - age, ttl: ttl));
            }
            return messages;
        }

        private static List<ServiceBusReceivedMessage> GenerateXmlBatch(int count)
        {
            var rng = new Random(123);
            var events = new[] { "sync.started", "sync.completed", "sync.failed", "cache.invalidated" };
            var messages = new List<ServiceBusReceivedMessage>();
            for (int i = 0; i < count; i++)
            {
                var evt = events[rng.Next(events.Length)];
                var age = TimeSpan.FromMinutes(rng.Next(5, 600));
                var body = $"<root><message>{evt}</message><seq>{i}</seq></root>";
                messages.Add(CreateMessage(body, "application/xml",
                    enqueuedTime: DateTimeOffset.UtcNow - age));
            }
            return messages;
        }

        private static List<ServiceBusReceivedMessage> GenerateMixedBatch(int count)
        {
            var all = new List<ServiceBusReceivedMessage>();
            int jsonCount = count / 2;
            int xmlCount = count / 4;
            int textCount = count - jsonCount - xmlCount;
            all.AddRange(GenerateJsonBatch(jsonCount));
            all.AddRange(GenerateXmlBatch(xmlCount));
            var rng = new Random(99);
            for (int i = 0; i < textCount; i++)
            {
                var age = TimeSpan.FromHours(rng.Next(1, 48));
                all.Add(CreateMessage($"plain text event {i}", contentType: null,
                    enqueuedTime: DateTimeOffset.UtcNow - age));
            }
            return all;
        }

        // ---------- functional tests ----------

        [Fact]
        public void FullPipeline_100JsonMessages_ProducesCompleteReport()
        {
            var messages = GenerateJsonBatch(100);
            var results = messages.Select(m => _analyzer.ProcessMessage(m)).ToList();
            var report = _analyzer.BuildAggregateReport(results, prefixLength: null, minCount: null);
            var formatted = _analyzer.FormatAggregateReport(report, "test-topic", "test-sub", "test-ns");

            _output.WriteLine(formatted);

            Assert.Equal(100, report.TotalMessages);
            Assert.True(report.UniqueExtractedValues > 0, "Should have grouped values");
            Assert.Contains("application/json", report.ContentTypeCounts.Keys);
            Assert.Equal(100, report.ContentTypeCounts["application/json"]);
            Assert.True(report.RiskBuckets.Values.Sum() > 0, "Should have risk distribution");
            Assert.NotNull(report.MinAgeSeconds);
            Assert.NotNull(report.MaxAgeSeconds);
            Assert.NotNull(report.MedianAgeSeconds);
            Assert.True(report.EnqueuedByDay.Count > 0, "Should have daily histogram");
            Assert.Contains("AGGREGATE ANALYSIS", formatted);
            Assert.Contains("CONTENT TYPE BREAKDOWN", formatted);
            Assert.Contains("RISK DISTRIBUTION", formatted);
            Assert.Contains("MESSAGE AGE", formatted);
            Assert.Contains("TOP EXTRACTED VALUES", formatted);
        }

        [Fact]
        public void FullPipeline_100XmlMessages_ProducesCompleteReport()
        {
            var messages = GenerateXmlBatch(100);
            var results = messages.Select(m => _analyzer.ProcessMessage(m)).ToList();
            var report = _analyzer.BuildAggregateReport(results, prefixLength: null, minCount: null);
            var formatted = _analyzer.FormatAggregateReport(report, "events-topic", "xml-sub", "prod-ns");

            _output.WriteLine(formatted);

            Assert.Equal(100, report.TotalMessages);
            Assert.Contains("application/xml", report.ContentTypeCounts.Keys);
            Assert.True(report.UniqueExtractedValues >= 2, "Should have multiple event types");
            // All XML messages should have valid extracted values
            var nonNull = results.Count(r => r.ExtractedValue != null && !r.ExtractedValue.StartsWith("["));
            Assert.True(nonNull > 90, $"Expected >90 successful XML extractions, got {nonNull}");
        }

        [Fact]
        public void FullPipeline_MixedContentTypes_BreaksDownCorrectly()
        {
            var messages = GenerateMixedBatch(120);
            var results = messages.Select(m => _analyzer.ProcessMessage(m)).ToList();
            var report = _analyzer.BuildAggregateReport(results, prefixLength: null, minCount: null);
            var formatted = _analyzer.FormatAggregateReport(report, "mixed-topic", "all-sub", "dev-ns");

            _output.WriteLine(formatted);

            Assert.Equal(120, report.TotalMessages);
            Assert.True(report.ContentTypeCounts.Count >= 2, "Should have multiple content types");
            int totalFromTypes = report.ContentTypeCounts.Values.Sum();
            Assert.Equal(120, totalFromTypes);
        }

        [Fact]
        public void FullPipeline_PrefixGrouping_ReducesCardinality()
        {
            var values = Enumerable.Range(0, 100)
                .Select(i => $"user-{i:D4}-event")
                .ToArray();
            var messages = GenerateJsonBatch(100, values);
            var results = messages.Select(m => _analyzer.ProcessMessage(m)).ToList();

            var reportFull = _analyzer.BuildAggregateReport(results, prefixLength: null, minCount: null);
            var reportPrefix = _analyzer.BuildAggregateReport(results, prefixLength: 6, minCount: null);

            _output.WriteLine($"Without prefix: {reportFull.UniqueExtractedValues} unique values");
            _output.WriteLine($"With prefix=6:  {reportPrefix.UniqueExtractedValues} unique values");

            Assert.True(reportPrefix.UniqueExtractedValues < reportFull.UniqueExtractedValues,
                "Prefix grouping should reduce number of unique values");
        }

        [Fact]
        public void FullPipeline_MinCountFilter_ExcludesRareValues()
        {
            // Create a skewed distribution: some values appear many times, some once
            var values = new List<string>();
            for (int i = 0; i < 40; i++) values.Add("FrequentEvent");
            for (int i = 0; i < 20; i++) values.Add("ModerateEvent");
            for (int i = 0; i < 10; i++) values.Add("RareEvent");
            for (int i = 0; i < 30; i++) values.Add($"Unique-{i}");

            var messages = GenerateJsonBatch(100, values.ToArray());
            var results = messages.Select(m => _analyzer.ProcessMessage(m)).ToList();

            var reportAll = _analyzer.BuildAggregateReport(results, prefixLength: null, minCount: null);
            var reportMin5 = _analyzer.BuildAggregateReport(results, prefixLength: null, minCount: 5);

            _output.WriteLine($"No filter: {reportAll.UniqueExtractedValues} groups");
            _output.WriteLine($"Min 5:     {reportMin5.UniqueExtractedValues} groups");

            Assert.True(reportMin5.UniqueExtractedValues < reportAll.UniqueExtractedValues,
                "Min count filter should exclude rare values");
            Assert.All(reportMin5.GroupedValues, kv =>
                Assert.True(kv.Value.count >= 5, $"'{kv.Key}' has count {kv.Value.count} < 5"));
        }

        [Fact]
        public void RiskCalculation_MessagesAcrossLifecycle_DistributeCorrectly()
        {
            var now = DateTimeOffset.UtcNow;
            var messages = new List<ServiceBusReceivedMessage>
            {
                // Fresh message (low risk)
                CreateMessage("{\"message\":\"fresh\"}", "application/json",
                    enqueuedTime: now.AddMinutes(-5), ttl: TimeSpan.FromDays(14)),
                // Half-life message (mid risk)
                CreateMessage("{\"message\":\"midlife\"}", "application/json",
                    enqueuedTime: now.AddDays(-7), ttl: TimeSpan.FromDays(14)),
                // Almost expired (high risk)
                CreateMessage("{\"message\":\"stale\"}", "application/json",
                    enqueuedTime: now.AddDays(-13), ttl: TimeSpan.FromDays(14)),
                // Already expired
                CreateMessage("{\"message\":\"expired\"}", "application/json",
                    enqueuedTime: now.AddDays(-15), ttl: TimeSpan.FromDays(14)),
            };

            var results = messages.Select(m => _analyzer.ProcessMessage(m)).ToList();
            var report = _analyzer.BuildAggregateReport(results, null, null);
            var formatted = _analyzer.FormatAggregateReport(report, "t", "s", "ns");

            _output.WriteLine(formatted);

            Assert.True(results[0].Risk < 5, $"Fresh message risk should be <5%, was {results[0].Risk:F1}%");
            Assert.True(results[1].Risk > 40 && results[1].Risk < 60, $"Midlife risk should be ~50%, was {results[1].Risk:F1}%");
            Assert.True(results[2].Risk > 85, $"Stale risk should be >85%, was {results[2].Risk:F1}%");
            Assert.Equal(100.0, results[3].Risk);
            Assert.True(report.ExpiredCount >= 1, "Should have at least 1 expired message");
        }

        [Fact]
        public void AgeStatistics_VariedAges_ComputeCorrectly()
        {
            var now = DateTimeOffset.UtcNow;
            var messages = new List<ServiceBusReceivedMessage>();
            // Create messages at known ages: 1h, 2h, 3h, ..., 10h
            for (int h = 1; h <= 10; h++)
            {
                messages.Add(CreateMessage($"{{\"message\":\"evt-{h}\"}}", "application/json",
                    enqueuedTime: now.AddHours(-h)));
            }

            var results = messages.Select(m => _analyzer.ProcessMessage(m)).ToList();
            var report = _analyzer.BuildAggregateReport(results, null, null);

            // Ages should be roughly 1h to 10h (3600s to 36000s)
            Assert.InRange(report.MinAgeSeconds.Value, 3500, 3700);
            Assert.InRange(report.MaxAgeSeconds.Value, 35900, 36100);

            // Median of 1..10 hours = 5.5 hours = 19800s
            Assert.InRange(report.MedianAgeSeconds.Value, 19500, 20100);

            _output.WriteLine($"Age range: {MessageAnalyzer.FormatDuration(report.MinAgeSeconds.Value)} - {MessageAnalyzer.FormatDuration(report.MaxAgeSeconds.Value)}");
            _output.WriteLine($"Median age: {MessageAnalyzer.FormatDuration(report.MedianAgeSeconds.Value)}");
        }

        [Fact]
        public void EnqueueHistogram_MultiDaySpread_ShowsDailyDistribution()
        {
            var now = DateTimeOffset.UtcNow;
            var messages = new List<ServiceBusReceivedMessage>();
            // 30 messages today, 40 yesterday, 30 two days ago
            for (int i = 0; i < 30; i++)
                messages.Add(CreateMessage($"{{\"message\":\"today-{i}\"}}", "application/json",
                    enqueuedTime: now.AddHours(-i % 12)));
            for (int i = 0; i < 40; i++)
                messages.Add(CreateMessage($"{{\"message\":\"yesterday-{i}\"}}", "application/json",
                    enqueuedTime: now.AddDays(-1).AddHours(-i % 12)));
            for (int i = 0; i < 30; i++)
                messages.Add(CreateMessage($"{{\"message\":\"2daysago-{i}\"}}", "application/json",
                    enqueuedTime: now.AddDays(-2).AddHours(-i % 12)));

            var results = messages.Select(m => _analyzer.ProcessMessage(m)).ToList();
            var report = _analyzer.BuildAggregateReport(results, null, null);
            var formatted = _analyzer.FormatAggregateReport(report, "t", "s", "ns");

            _output.WriteLine(formatted);

            Assert.Equal(100, report.TotalMessages);
            Assert.True(report.EnqueuedByDay.Count >= 3, "Should span at least 3 days");
            Assert.Equal(100, report.EnqueuedByDay.Values.Sum());
        }

        [Fact]
        public void FormatDuration_VariousScales_FormatsReadably()
        {
            Assert.Equal("30s", MessageAnalyzer.FormatDuration(30));
            Assert.Equal("5.0m", MessageAnalyzer.FormatDuration(300));
            Assert.Equal("2.5h", MessageAnalyzer.FormatDuration(9000));
            Assert.Equal("3.0d", MessageAnalyzer.FormatDuration(259200));
        }

        [Fact]
        public void ContentDetection_NoContentType_AutoDetects()
        {
            var jsonMsg = CreateMessage("{\"message\": \"auto-detected\"}", contentType: null);
            var xmlMsg = CreateMessage("<root><message>auto-xml</message></root>", contentType: null);
            var textMsg = CreateMessage("just plain text here", contentType: null);

            var jsonResult = _analyzer.ProcessMessage(jsonMsg);
            var xmlResult = _analyzer.ProcessMessage(xmlMsg);
            var textResult = _analyzer.ProcessMessage(textMsg);

            Assert.Equal("json", jsonResult.ContentType);
            Assert.Equal("auto-detected", jsonResult.ExtractedValue);

            Assert.Equal("xml", xmlResult.ContentType);
            Assert.Equal("auto-xml", xmlResult.ExtractedValue);

            Assert.Equal("text", textResult.ContentType);
        }

        [Fact]
        public void FullPipeline_LargeVolume_200Messages_PerformsWithinBounds()
        {
            var sw = System.Diagnostics.Stopwatch.StartNew();
            var messages = GenerateMixedBatch(200);
            var results = messages.Select(m => _analyzer.ProcessMessage(m)).ToList();
            var report = _analyzer.BuildAggregateReport(results, prefixLength: 10, minCount: 2);
            var formatted = _analyzer.FormatAggregateReport(report, "perf-topic", "perf-sub", "perf-ns");
            sw.Stop();

            _output.WriteLine($"200-message pipeline completed in {sw.ElapsedMilliseconds}ms");
            _output.WriteLine(formatted);

            Assert.Equal(200, report.TotalMessages);
            Assert.True(sw.ElapsedMilliseconds < 5000, $"Pipeline took {sw.ElapsedMilliseconds}ms, expected < 5s");
        }
    }
}
