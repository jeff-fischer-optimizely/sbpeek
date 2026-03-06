# sbpeek

A .NET CLI tool that non-destructively peeks into Azure Service Bus topics and produces aggregate analysis of message patterns.

## What it does

- Peeks messages from a Service Bus topic/subscription without consuming them
- Deduplicates by MessageId
- Auto-detects content type (JSON, XML, text, binary)
- Extracts key values from message bodies
- Produces an aggregate report including:
  - Content type breakdown
  - Risk-to-expire distribution (how close messages are to TTL expiry)
  - Message age statistics (min, max, average, median)
  - Enqueue volume histograms (by day and hour)
  - Grouped extracted values with counts
- Optionally dumps raw messages to JSON for further analysis

## Prerequisites

- [.NET 8 SDK](https://dotnet.microsoft.com/download/dotnet/8.0)
- Azure CLI logged in (`az login`) — authentication uses `AzureCliCredential`
- Read access to the target Service Bus namespace

## Build

```bash
dotnet build
```

## Usage

```bash
dotnet run -- --namespace <sb-namespace> --subscription_name <sub> [options]
```

### Required options

| Option | Description |
|--------|-------------|
| `--namespace` | Azure Service Bus namespace (without `.servicebus.windows.net`) |
| `--subscription_name` | Subscription name to peek |

### Optional options

| Option | Default | Description |
|--------|---------|-------------|
| `--topic_name` | `mysiteevents` | Topic name |
| `--message_sample` | `256` | Number of messages to collect |
| `--polling` | `2` | Polling interval in seconds between peek batches |
| `--output <file>` | stdout | Write the aggregate report to a file |
| `--dump <file>` | — | Dump raw peeked messages to a JSON file |
| `--prefix-length <n>` | — | Group extracted values by first N characters |
| `--min-count <n>` | — | Only show groups with at least N occurrences |
| `--log_level` | `Error` | Logging level |

### Examples

```bash
# Peek 100 messages and print the aggregate report
dotnet run -- --namespace my-sb-ns --subscription_name my-sub --message_sample 100

# Save report to file
dotnet run -- --namespace my-sb-ns --subscription_name my-sub --output report.txt

# Dump raw messages for offline analysis (e.g. feeding into Claude)
dotnet run -- --namespace my-sb-ns --subscription_name my-sub --dump messages.json

# Group by first 8 characters, only show values appearing 3+ times
dotnet run -- --namespace my-sb-ns --subscription_name my-sub --prefix-length 8 --min-count 3
```

## Sample output

```
========================================================================================================================
  AGGREGATE ANALYSIS - 100 messages
  Topic: mysiteevents | Subscription: my-sub | Namespace: my-sb-ns
========================================================================================================================

  CONTENT TYPE BREAKDOWN
--------------------------------------------------
  application/json   100 (100.0%) ##################################################

  RISK DISTRIBUTION (% of TTL elapsed)
--------------------------------------------------
  0-25%                 72 ( 72.0%) ####################################
  25-50%                20 ( 20.0%) ##########
  50-75%                 5 (  5.0%) ##
  75-99%                 3 (  3.0%) #
  Expired (100%)         0 (  0.0%)

  Risk stats: Min=1.2%  Max=89.4%  Avg=18.7%  Expired=0

  MESSAGE AGE
--------------------------------------------------
  Youngest: 6.0m
  Oldest:   2.8d
  Average:  14.2h
  Median:   10.5h

  ENQUEUE VOLUME BY DAY
--------------------------------------------------
  2026-03-04     22  ######################
  2026-03-05     43  ########################################
  2026-03-06     35  ################################

  TOP EXTRACTED VALUES
------------------------------------------------------------------------------------------------------------------------
  Count    | Risk to expire % | Extracted Value
------------------------------------------------------------------------------------------------------------------------
  36       |              1.4 | sync.started
  22       |              1.2 | cache.invalidated
  21       |              1.1 | sync.failed
  21       |              1.5 | sync.completed
------------------------------------------------------------------------------------------------------------------------
  4 unique extracted values

========================================================================================================================
```

## Dump format

The `--dump` option writes a JSON array with full message metadata:

```json
[
  {
    "MessageId": "abc-123",
    "ContentType": "application/json",
    "Subject": null,
    "CorrelationId": null,
    "EnqueuedTime": "2026-03-06T10:30:00.0000000+00:00",
    "ExpiresAt": "2026-03-20T10:30:00.0000000+00:00",
    "TimeToLive": "14.00:00:00",
    "DeliveryCount": 1,
    "SequenceNumber": 42,
    "ApplicationProperties": { "eventType": "UserCreated" },
    "Body": "{\"message\": \"UserCreated\", \"id\": 1}"
  }
]
```

## Tests

```bash
dotnet test
```

11 functional tests exercise the full pipeline (message processing, grouping, aggregation, formatting) using `ServiceBusModelFactory` to create realistic messages without requiring a live Service Bus connection.

## Project structure

```
ServiceBusAnalyzer.cs          # CLI entry point, Service Bus peeking, message dump
MessageAnalyzer.cs             # Analysis engine: processing, grouping, aggregation, formatting
ServiceBusAnalyzer.Tests/
  FunctionalTests.cs           # 11 functional tests covering the full pipeline
```
