using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Xml.Linq;
using Azure.Messaging.ServiceBus;

namespace ServiceBusAnalyzer
{
    public class MessageResult
    {
        public string ExtractedValue { get; set; }
        public double? Risk { get; set; }
        public double? TimeRemainingSeconds { get; set; }
        public string Body { get; set; }
        public string ContentType { get; set; }
        public DateTimeOffset? EnqueuedTime { get; set; }
        public DateTimeOffset? ExpiresAt { get; set; }
    }

    public class AggregateReport
    {
        public int TotalMessages { get; set; }
        public int UniqueExtractedValues { get; set; }
        public Dictionary<string, int> ContentTypeCounts { get; set; } = new();
        public Dictionary<string, int> RiskBuckets { get; set; } = new();
        public double? MinAgeSeconds { get; set; }
        public double? MaxAgeSeconds { get; set; }
        public double? AvgAgeSeconds { get; set; }
        public double? MedianAgeSeconds { get; set; }
        public double? MinRisk { get; set; }
        public double? MaxRisk { get; set; }
        public double? AvgRisk { get; set; }
        public int ExpiredCount { get; set; }
        public Dictionary<string, int> EnqueuedByHour { get; set; } = new();
        public Dictionary<string, int> EnqueuedByDay { get; set; } = new();
        public Dictionary<string, (int count, List<double> risks)> GroupedValues { get; set; } = new();
    }

    public class MessageAnalyzer
    {
        public MessageResult ProcessMessage(ServiceBusReceivedMessage msg)
        {
            string contentType = msg.ContentType?.ToLower() ?? "";
            byte[] bodyBytes = msg.Body.ToArray();
            string bodyText = Encoding.UTF8.GetString(bodyBytes);
            DateTimeOffset? enqueuedAt = msg.EnqueuedTime;
            DateTimeOffset? expiresAt = msg.ExpiresAt;
            double? risk = null;
            double? timeRemaining = null;
            if (enqueuedAt.HasValue && expiresAt.HasValue)
            {
                risk = CalculateRisk(enqueuedAt.Value, expiresAt.Value);
                timeRemaining = CalculateTimeRemaining(expiresAt.Value);
            }
            if (string.IsNullOrEmpty(contentType))
                contentType = DetectFormat(bodyBytes);
            string extractedValue = null;
            if (contentType.Contains("json"))
                extractedValue = HandleJsonMessage(bodyText);
            else if (contentType.Contains("xml"))
                extractedValue = HandleXmlMessage(bodyText);
            else
                extractedValue = ExtractMessageBody(bodyText);
            return new MessageResult
            {
                ExtractedValue = extractedValue,
                Risk = risk,
                TimeRemainingSeconds = timeRemaining,
                Body = bodyText,
                ContentType = contentType,
                EnqueuedTime = enqueuedAt,
                ExpiresAt = expiresAt
            };
        }

        public string ExtractMessageBody(string message)
        {
            try
            {
                var pattern = @"XMLSchema[^a-zA-Z]{0,3}([^@]+)@";
                var matches = Regex.Matches(message, pattern);
                if (matches.Count >= 2)
                    return matches[1].Groups[1].Value.Trim();
            }
            catch { }
            return null;
        }

        public string DetectFormat(byte[] bodyBytes)
        {
            string text;
            try { text = Encoding.UTF8.GetString(bodyBytes); }
            catch { return "binary"; }
            try { JsonDocument.Parse(text); return "json"; } catch { }
            try { XDocument.Parse(text); return "xml"; } catch { }
            return "text";
        }

        public double CalculateRisk(DateTimeOffset enqueuedAt, DateTimeOffset expiresAt)
        {
            var now = DateTimeOffset.UtcNow;
            var totalLifetime = (expiresAt - enqueuedAt).TotalSeconds;
            var timeLeft = (expiresAt - now).TotalSeconds;
            if (timeLeft <= 0) return 100.0;
            if (totalLifetime <= 0) return 0.0;
            return Math.Max(0.0, Math.Min(100.0, (1 - timeLeft / totalLifetime) * 100));
        }

        public double CalculateTimeRemaining(DateTimeOffset expiresAt)
        {
            var now = DateTimeOffset.UtcNow;
            var remaining = (expiresAt - now).TotalSeconds;
            return Math.Max(0.0, remaining);
        }

        public string HandleJsonMessage(string text)
        {
            try
            {
                var obj = JsonDocument.Parse(text);
                if (obj.RootElement.TryGetProperty("message", out var message))
                    return message.GetString();
            }
            catch { }
            return "[Invalid JSON]";
        }

        public string HandleXmlMessage(string text)
        {
            try
            {
                var doc = XDocument.Parse(text);
                var elem = doc.Root.Element("message");
                return elem != null ? elem.Value : "[No <message> element found]";
            }
            catch { }
            return "[Invalid XML]";
        }

        public Dictionary<string, (int count, List<double> risks)> GroupMessages(List<MessageResult> results, int? prefixLength, int? minCount)
        {
            var grouped = new Dictionary<string, (int count, List<double> risks)>();
            foreach (var result in results)
            {
                var val = result?.ExtractedValue;
                if (string.IsNullOrWhiteSpace(val)) continue;
                val = val.Trim();
                var risk = result?.Risk;
                string key = prefixLength.HasValue ? (val.Length > prefixLength ? val.Substring(0, prefixLength.Value) + "..." : val) : val;
                if (!grouped.ContainsKey(key)) grouped[key] = (0, new List<double>());
                grouped[key] = (grouped[key].count + 1, risk.HasValue ? grouped[key].risks.Append(risk.Value).ToList() : grouped[key].risks);
            }
            if (minCount.HasValue)
                grouped = grouped.Where(kv => kv.Value.count >= minCount.Value).ToDictionary(kv => kv.Key, kv => kv.Value);
            return grouped;
        }

        public AggregateReport BuildAggregateReport(List<MessageResult> results, int? prefixLength, int? minCount)
        {
            var report = new AggregateReport
            {
                TotalMessages = results.Count,
                GroupedValues = GroupMessages(results, prefixLength, minCount),
            };
            report.UniqueExtractedValues = report.GroupedValues.Count;

            // Content type breakdown
            foreach (var r in results)
            {
                var ct = r.ContentType ?? "unknown";
                report.ContentTypeCounts[ct] = report.ContentTypeCounts.GetValueOrDefault(ct) + 1;
            }

            // Risk analysis
            var risks = results.Where(r => r.Risk.HasValue).Select(r => r.Risk.Value).ToList();
            if (risks.Count > 0)
            {
                report.MinRisk = risks.Min();
                report.MaxRisk = risks.Max();
                report.AvgRisk = risks.Average();
                report.ExpiredCount = risks.Count(r => r >= 100.0);

                report.RiskBuckets["0-25%"] = risks.Count(r => r >= 0 && r < 25);
                report.RiskBuckets["25-50%"] = risks.Count(r => r >= 25 && r < 50);
                report.RiskBuckets["50-75%"] = risks.Count(r => r >= 50 && r < 75);
                report.RiskBuckets["75-99%"] = risks.Count(r => r >= 75 && r < 100);
                report.RiskBuckets["Expired (100%)"] = risks.Count(r => r >= 100);
            }

            // Message age analysis
            var now = DateTimeOffset.UtcNow;
            var ages = results
                .Where(r => r.EnqueuedTime.HasValue)
                .Select(r => (now - r.EnqueuedTime.Value).TotalSeconds)
                .Where(a => a >= 0)
                .OrderBy(a => a)
                .ToList();

            if (ages.Count > 0)
            {
                report.MinAgeSeconds = ages.Min();
                report.MaxAgeSeconds = ages.Max();
                report.AvgAgeSeconds = ages.Average();
                report.MedianAgeSeconds = ages.Count % 2 == 0
                    ? (ages[ages.Count / 2 - 1] + ages[ages.Count / 2]) / 2.0
                    : ages[ages.Count / 2];
            }

            // Enqueue time histograms
            foreach (var r in results.Where(r => r.EnqueuedTime.HasValue))
            {
                var t = r.EnqueuedTime.Value;
                var hourKey = t.ToString("yyyy-MM-dd HH:00");
                var dayKey = t.ToString("yyyy-MM-dd");
                report.EnqueuedByHour[hourKey] = report.EnqueuedByHour.GetValueOrDefault(hourKey) + 1;
                report.EnqueuedByDay[dayKey] = report.EnqueuedByDay.GetValueOrDefault(dayKey) + 1;
            }

            return report;
        }

        public string FormatAggregateReport(AggregateReport report, string topic, string subscription, string namespaceName)
        {
            const int WIDTH = 120;
            var sb = new StringBuilder();

            sb.AppendLine(new string('=', WIDTH));
            sb.AppendLine($"  AGGREGATE ANALYSIS — {report.TotalMessages} messages");
            sb.AppendLine($"  Topic: {topic} | Subscription: {subscription} | Namespace: {namespaceName}");
            sb.AppendLine(new string('=', WIDTH));

            // Content type breakdown
            sb.AppendLine();
            sb.AppendLine("  CONTENT TYPE BREAKDOWN");
            sb.AppendLine(new string('-', 50));
            foreach (var kv in report.ContentTypeCounts.OrderByDescending(x => x.Value))
            {
                var pct = (double)kv.Value / report.TotalMessages * 100;
                var bar = new string('#', (int)(pct / 2));
                sb.AppendLine($"  {kv.Key,-15} {kv.Value,6} ({pct,5:F1}%) {bar}");
            }

            // Risk distribution
            if (report.RiskBuckets.Count > 0)
            {
                sb.AppendLine();
                sb.AppendLine("  RISK DISTRIBUTION (% of TTL elapsed)");
                sb.AppendLine(new string('-', 50));
                foreach (var kv in report.RiskBuckets)
                {
                    var pct = report.TotalMessages > 0 ? (double)kv.Value / report.TotalMessages * 100 : 0;
                    var bar = new string('#', (int)(pct / 2));
                    sb.AppendLine($"  {kv.Key,-18} {kv.Value,6} ({pct,5:F1}%) {bar}");
                }
                sb.AppendLine();
                sb.AppendLine($"  Risk stats: Min={report.MinRisk:F1}%  Max={report.MaxRisk:F1}%  Avg={report.AvgRisk:F1}%  Expired={report.ExpiredCount}");
            }

            // Message age
            if (report.MinAgeSeconds.HasValue)
            {
                sb.AppendLine();
                sb.AppendLine("  MESSAGE AGE");
                sb.AppendLine(new string('-', 50));
                sb.AppendLine($"  Youngest: {FormatDuration(report.MinAgeSeconds.Value)}");
                sb.AppendLine($"  Oldest:   {FormatDuration(report.MaxAgeSeconds.Value)}");
                sb.AppendLine($"  Average:  {FormatDuration(report.AvgAgeSeconds.Value)}");
                sb.AppendLine($"  Median:   {FormatDuration(report.MedianAgeSeconds.Value)}");
            }

            // Enqueue histogram by day
            if (report.EnqueuedByDay.Count > 0)
            {
                sb.AppendLine();
                sb.AppendLine("  ENQUEUE VOLUME BY DAY");
                sb.AppendLine(new string('-', 50));
                int maxDayCount = report.EnqueuedByDay.Values.Max();
                foreach (var kv in report.EnqueuedByDay.OrderBy(x => x.Key))
                {
                    int barLen = maxDayCount > 0 ? (int)((double)kv.Value / maxDayCount * 40) : 0;
                    sb.AppendLine($"  {kv.Key}  {kv.Value,5}  {new string('#', barLen)}");
                }
            }

            // Enqueue histogram by hour (only if <= 48 hours of data)
            if (report.EnqueuedByHour.Count > 0 && report.EnqueuedByHour.Count <= 48)
            {
                sb.AppendLine();
                sb.AppendLine("  ENQUEUE VOLUME BY HOUR");
                sb.AppendLine(new string('-', 50));
                int maxHourCount = report.EnqueuedByHour.Values.Max();
                foreach (var kv in report.EnqueuedByHour.OrderBy(x => x.Key))
                {
                    int barLen = maxHourCount > 0 ? (int)((double)kv.Value / maxHourCount * 40) : 0;
                    sb.AppendLine($"  {kv.Key}  {kv.Value,5}  {new string('#', barLen)}");
                }
            }

            // Top grouped values (existing table)
            sb.AppendLine();
            sb.AppendLine("  TOP EXTRACTED VALUES");
            const int COUNT_WIDTH = 8;
            const int RISK_WIDTH = 16;
            int VALUE_WIDTH = WIDTH - COUNT_WIDTH - RISK_WIDTH - 7;
            sb.AppendLine(new string('-', WIDTH));
            sb.AppendLine($"  {"Count",-COUNT_WIDTH} | {"Risk to expire %",-RISK_WIDTH} | Extracted Value");
            sb.AppendLine(new string('-', WIDTH));
            foreach (var kv in report.GroupedValues.OrderByDescending(x => x.Value.count))
            {
                var count = kv.Value.count;
                var risks = kv.Value.risks;
                var avgRisk = risks.Count > 0 ? risks.Average() : 0;
                var cleanVal = kv.Key.Replace("\n", " ").Replace("\r", " ");
                var truncatedVal = cleanVal.Length > VALUE_WIDTH ? cleanVal.Substring(0, VALUE_WIDTH - 3) + "..." : cleanVal;
                sb.AppendLine($"  {count,-COUNT_WIDTH} | {avgRisk,RISK_WIDTH:F1} | {truncatedVal}");
            }
            sb.AppendLine(new string('-', WIDTH));
            sb.AppendLine($"  {report.UniqueExtractedValues} unique extracted values");

            sb.AppendLine();
            sb.AppendLine(new string('=', WIDTH));
            return sb.ToString();
        }

        public static string FormatDuration(double seconds)
        {
            if (seconds < 60) return $"{seconds:F0}s";
            if (seconds < 3600) return $"{seconds / 60:F1}m";
            if (seconds < 86400) return $"{seconds / 3600:F1}h";
            return $"{seconds / 86400:F1}d";
        }
    }
}
