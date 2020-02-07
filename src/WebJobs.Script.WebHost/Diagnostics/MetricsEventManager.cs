﻿// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs.Script.Configuration;
using Microsoft.Azure.WebJobs.Script.Description;
using Microsoft.Azure.WebJobs.Script.Diagnostics;
using Microsoft.Azure.WebJobs.Script.WebHost.ContainerManagement;
using Microsoft.Azure.WebJobs.Script.WebHost.Metrics;
using Microsoft.Azure.WebJobs.Script.WebHost.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Microsoft.Azure.WebJobs.Script.WebHost.Diagnostics
{
    public class MetricsEventManager : IDisposable
    {
        // Default time between flushes in seconds (every 30 seconds)
        private const int DefaultFlushIntervalMS = 30 * 1000;

        private readonly FunctionActivityTracker _functionActivityTracker = null;
        private readonly IEventGenerator _eventGenerator;
        private readonly int _functionActivityFlushIntervalSeconds;
        private readonly Timer _metricsFlushTimer;
        private readonly ILogger<MetricsEventManager> _logger;
        private bool _disposed;
        private IOptionsMonitor<AppServiceOptions> _appServiceOptions;

        public MetricsEventManager(IOptionsMonitor<AppServiceOptions> appServiceOptions, IEventGenerator generator, int functionActivityFlushIntervalSeconds, IMetricsPublisher metricsPublisher, ILinuxContainerActivityPublisher linuxContainerActivityPublisher, ILogger<MetricsEventManager> logger, int metricsFlushIntervalMS = DefaultFlushIntervalMS)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));

            // we read these in the ctor (not static ctor) since it can change on the fly
            _appServiceOptions = appServiceOptions;
            _eventGenerator = generator;
            _functionActivityFlushIntervalSeconds = functionActivityFlushIntervalSeconds;
            QueuedEvents = new ConcurrentDictionary<string, SystemMetricEvent>(StringComparer.OrdinalIgnoreCase);

            // Initialize the periodic log flush timer
            _metricsFlushTimer = new Timer(TimerFlush, null, metricsFlushIntervalMS, metricsFlushIntervalMS);

            _functionActivityTracker = new FunctionActivityTracker(_appServiceOptions, _eventGenerator, metricsPublisher, linuxContainerActivityPublisher, _functionActivityFlushIntervalSeconds);
        }

        /// <summary>
        /// Gets the collection of events that will be flushed on the next flush interval.
        /// </summary>
        public ConcurrentDictionary<string, SystemMetricEvent> QueuedEvents { get; }

        public object BeginEvent(string eventName, string functionName = null, string data = null)
        {
            if (string.IsNullOrEmpty(eventName))
            {
                throw new ArgumentNullException(nameof(eventName));
            }

            return new SystemMetricEvent
            {
                FunctionName = functionName,
                EventName = eventName.ToLowerInvariant(),
                Timestamp = DateTime.UtcNow,
                StopWatch = Stopwatch.StartNew(),
                Data = data
            };
        }

        public void EndEvent(object eventHandle)
        {
            if (eventHandle == null)
            {
                throw new ArgumentNullException(nameof(eventHandle));
            }

            SystemMetricEvent evt = eventHandle as SystemMetricEvent;
            if (evt != null)
            {
                long latencyMS = 0;
                evt.StopWatch.Stop();
                if (evt.StopWatch != null)
                {
                    evt.Duration = evt.StopWatch.Elapsed;
                    latencyMS = evt.StopWatch.ElapsedMilliseconds;
                }
                else
                {
                    evt.Duration = DateTime.UtcNow - evt.Timestamp;
                    latencyMS = (long)evt.Duration.TotalMilliseconds;
                }

                // event aggregation is based on this key
                // for each unique key, there will be only 1
                // queued event that we aggregate into
                string key = GetAggregateKey(evt.EventName, evt.FunctionName);

                QueuedEvents.AddOrUpdate(key,
                    (name) =>
                    {
                        // create the default event that will be added
                        // if an event isn't already queued for this key
                        return new SystemMetricEvent
                        {
                            FunctionName = evt.FunctionName,
                            EventName = evt.EventName,
                            Minimum = latencyMS,
                            Maximum = latencyMS,
                            Average = latencyMS,
                            Count = 1,
                            Data = evt.Data
                        };
                    },
                    (name, evtToUpdate) =>
                    {
                        // Aggregate into the existing event
                        // While we'll be performing an aggregation later,
                        // we retain the count so weighted averages can be performed
                        evtToUpdate.Maximum = Math.Max(evtToUpdate.Maximum, latencyMS);
                        evtToUpdate.Minimum = Math.Min(evtToUpdate.Minimum, latencyMS);
                        evtToUpdate.Average += latencyMS;  // the average is calculated later - for now we sum
                        evtToUpdate.Count++;

                        return evtToUpdate;
                    });
            }
        }

        public void LogEvent(string eventName, string functionName = null, string data = null)
        {
            if (string.IsNullOrEmpty(eventName))
            {
                throw new ArgumentNullException(nameof(eventName));
            }

            string key = GetAggregateKey(eventName, functionName);
            QueuedEvents.AddOrUpdate(key,
                (name) =>
                {
                    // create the default event that will be added
                    // if an event isn't already queued for this key
                    return new SystemMetricEvent
                    {
                        FunctionName = functionName,
                        EventName = eventName.ToLowerInvariant(),
                        Count = 1,
                        Data = data
                    };
                },
                (name, evtToUpdate) =>
                {
                    // update the existing event
                    evtToUpdate.Count++;

                    return evtToUpdate;
                });
        }

        internal void FunctionStarted(FunctionStartedEvent startedEvent)
        {
            _functionActivityTracker.FunctionStarted(startedEvent);
        }

        internal void FunctionCompleted(FunctionStartedEvent completedEvent)
        {
            _functionActivityTracker.FunctionCompleted(completedEvent);
        }

        internal void HostStarted(ScriptHost scriptHost)
        {
            if (scriptHost == null || scriptHost.Functions == null)
            {
                return;
            }

            foreach (var function in scriptHost.Functions)
            {
                if (function == null || function.Metadata == null)
                {
                    continue;
                }

                _eventGenerator.LogFunctionDetailsEvent(
                    _appServiceOptions.CurrentValue.AppName,
                    GetNormalizedString(function.Name),
                    function.Metadata != null ? SerializeBindings(function.Metadata.InputBindings) : GetNormalizedString(null),
                    function.Metadata != null ? SerializeBindings(function.Metadata.OutputBindings) : GetNormalizedString(null),
                    function.Metadata.Language,
                    function.Metadata != null ? function.Metadata.IsDisabled : false);
            }
        }

        /// <summary>
        /// Constructs the aggregate key used to group events. When metric events are
        /// added for later aggregation on flush, they'll be grouped by this key.
        /// </summary>
        internal static string GetAggregateKey(string eventName, string functionName = null)
        {
            string key = string.IsNullOrEmpty(functionName) ?
                eventName : $"{eventName}_{functionName}";

            return key.ToLowerInvariant();
        }

        private static string SerializeBindings(IEnumerable<BindingMetadata> bindings)
        {
            if (bindings != null)
            {
                return string.Join(",", bindings.ToList().Select(b => b.Type.ToString()));
            }
            else
            {
                return GetNormalizedString(null);
            }
        }

        private static string GetNormalizedString(string input)
        {
            return input ?? string.Empty;
        }

        public void Flush()
        {
            _functionActivityTracker.Flush();

            FlushMetrics();
        }

        private void FlushMetrics()
        {
            if (QueuedEvents.Count == 0)
            {
                return;
            }

            SystemMetricEvent[] eventsToFlush = QueuedEvents.Values.ToArray();
            QueuedEvents.Clear();

            // Use the same timestamp for all events. Since these are
            // aggregated events, individual timestamps for when the events were
            // started are meaningless
            DateTime eventTimestamp = DateTime.UtcNow;

            foreach (SystemMetricEvent evt in eventsToFlush)
            {
                evt.Timestamp = eventTimestamp;

                // perform the average calculation that we have postponed
                evt.Average /= evt.Count;
            }

            WriteMetricEvents(eventsToFlush);
        }

        /// <summary>
        /// Flush any queued events to event source immediately.
        /// </summary>
        /// <remarks>This method may run concurrently with itself so ensure there are no
        /// unintended side effects or race conditions within the implementation.</remarks>
        protected internal virtual void TimerFlush(object state)
        {
            FlushMetrics();
        }

        protected internal virtual void WriteMetricEvents(SystemMetricEvent[] metricEvents)
        {
            if (metricEvents == null)
            {
                throw new ArgumentNullException(nameof(metricEvents));
            }

            AppServiceOptions currentAppServiceOptions = _appServiceOptions.CurrentValue;
            foreach (SystemMetricEvent metricEvent in metricEvents)
            {
                _eventGenerator.LogFunctionMetricEvent(
                    currentAppServiceOptions.SubscriptionId,
                    currentAppServiceOptions.AppName,
                    metricEvent.FunctionName ?? string.Empty,
                    metricEvent.EventName.ToLowerInvariant(),
                    metricEvent.Average,
                    metricEvent.Minimum,
                    metricEvent.Maximum,
                    metricEvent.Count,
                    metricEvent.Timestamp,
                    metricEvent.Data ?? string.Empty,
                    currentAppServiceOptions.RuntimeSiteName ?? string.Empty,
                    currentAppServiceOptions.SlotName ?? string.Empty);
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            try
            {
                _logger.LogDebug($"Disposing {nameof(MetricsEventManager)}");
            }
            catch
            {
                // Best effort logging.
            }

            if (!_disposed)
            {
                if (disposing)
                {
                    // flush any outstanding events
                    TimerFlush(state: null);

                    if (_metricsFlushTimer != null)
                    {
                        _metricsFlushTimer.Dispose();
                    }

                    if (_functionActivityTracker != null)
                    {
                        _functionActivityTracker.StopEtwTaskAndRaiseFinishedEvent();
                        _functionActivityTracker.Dispose();
                    }
                }

                _disposed = true;
            }
        }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private class FunctionActivityTracker : IDisposable
        {
            private readonly IMetricsPublisher _metricsPublisher;
            private readonly ILinuxContainerActivityPublisher _linuxContainerActivityPublisher;

            private ulong _totalExecutionCount = 0;
            private int _functionActivityFlushInterval;
            private CancellationTokenSource _etwTaskCancellationSource = new CancellationTokenSource();
            private ConcurrentQueue<FunctionMetrics> _functionMetricsQueue = new ConcurrentQueue<FunctionMetrics>();
            private ConcurrentDictionary<string, RunningFunctionInfo> _runningFunctions = new ConcurrentDictionary<string, RunningFunctionInfo>();
            private bool _disposed = false;
            private IOptionsMonitor<AppServiceOptions> _appServiceOptions;

            // This ID is just an event grouping mechanism that can be used by event consimers
            // to group events coming from the same app host.
            private string _executionId = Guid.NewGuid().ToString();

            internal FunctionActivityTracker(IOptionsMonitor<AppServiceOptions> appServiceOptions, IEventGenerator generator, IMetricsPublisher metricsPublisher, ILinuxContainerActivityPublisher linuxContainerActivityPublisher, int functionActivityFlushInterval)
            {
                MetricsEventGenerator = generator;
                _appServiceOptions = appServiceOptions;
                _functionActivityFlushInterval = functionActivityFlushInterval;

                if (linuxContainerActivityPublisher != null && linuxContainerActivityPublisher != NullLinuxContainerActivityPublisher.Instance)
                {
                    _linuxContainerActivityPublisher = linuxContainerActivityPublisher;
                }

                if (metricsPublisher != null && metricsPublisher != NullMetricsPublisher.Instance)
                {
                    _metricsPublisher = metricsPublisher;
                }

                Task.Run(
                    async () =>
                    {
                        try
                        {
                            int currentSecond = _functionActivityFlushInterval;
                            while (!_etwTaskCancellationSource.Token.IsCancellationRequested)
                            {
                                RaiseMetricsPerFunctionEvent();

                                if (currentSecond >= _functionActivityFlushInterval)
                                {
                                    RaiseFunctionMetricEvents();
                                    currentSecond = 0;
                                }
                                else
                                {
                                    currentSecond = currentSecond + 1;
                                }

                                await Task.Delay(TimeSpan.FromSeconds(1), _etwTaskCancellationSource.Token);
                            }
                        }
                        catch (TaskCanceledException)
                        {
                            // This exception gets throws when cancellation request is raised via cancellation token.
                            // Let's eat this exception and continue
                        }
                    },
                    _etwTaskCancellationSource.Token);
            }

            internal bool IsActive
            {
                get
                {
                    return _runningFunctions.Count != 0;
                }
            }

            internal IEventGenerator MetricsEventGenerator { get; private set; }

            protected virtual void Dispose(bool disposing)
            {
                if (!_disposed)
                {
                    if (disposing)
                    {
                        _etwTaskCancellationSource.Dispose();
                    }
                    _disposed = true;
                }
            }

            public void Dispose()
            {
                Dispose(true);
                GC.SuppressFinalize(this);
            }

            internal void FunctionStarted(FunctionStartedEvent startedEvent)
            {
                _totalExecutionCount++;

                var monitoringEvent = new FunctionMetrics(startedEvent.FunctionMetadata.Name, ExecutionStage.Started, 0);
                _functionMetricsQueue.Enqueue(monitoringEvent);

                var key = GetDictionaryKey(startedEvent.FunctionMetadata.Name, startedEvent.InvocationId);
                var triggerType = startedEvent.FunctionMetadata.Trigger?.Type;
                var value = new RunningFunctionInfo(startedEvent.FunctionMetadata.Name, startedEvent.InvocationId, startedEvent.Timestamp, startedEvent.Success, triggerType);
                _runningFunctions.AddOrUpdate(key, value, (k, v) => value);
            }

            internal void FunctionCompleted(FunctionStartedEvent startedEvent)
            {
                var functionStage = (startedEvent.Success == false) ? ExecutionStage.Failed : ExecutionStage.Succeeded;
                long executionTimeInMS = (long)startedEvent.Duration.TotalMilliseconds;
                var monitoringEvent = new FunctionMetrics(startedEvent.FunctionMetadata.Name, functionStage, executionTimeInMS);
                _functionMetricsQueue.Enqueue(monitoringEvent);

                var key = GetDictionaryKey(startedEvent.FunctionMetadata.Name, startedEvent.InvocationId);
                if (_runningFunctions.TryRemove(key, out RunningFunctionInfo functionInfo))
                {
                    functionInfo.ExecutionStage = ExecutionStage.Finished;
                    functionInfo.Success = startedEvent.Success;
                    var endTime = startedEvent.Timestamp + startedEvent.Duration;
                    functionInfo.EndTime = startedEvent.Timestamp + startedEvent.Duration;

                    RaiseFunctionMetricEvent(functionInfo, _runningFunctions.Keys.Count, endTime);
                }
            }

            internal void StopEtwTaskAndRaiseFinishedEvent()
            {
                _etwTaskCancellationSource.Cancel();
                RaiseMetricsPerFunctionEvent();
            }

            internal void Flush()
            {
                RaiseMetricsPerFunctionEvent();
                RaiseFunctionMetricEvents();
            }

            private void RaiseFunctionMetricEvents()
            {
                var currentTime = DateTime.UtcNow;
                var runningFunctions = _runningFunctions.Values.ToArray();
                foreach (var runningFunction in runningFunctions)
                {
                    RaiseFunctionMetricEvent(runningFunction, runningFunctions.Length, currentTime);
                }
            }

            private void RaiseFunctionMetricEvent(RunningFunctionInfo runningFunctionInfo, int concurrency, DateTime currentTime)
            {
                double executionTimespan = 0;
                if (runningFunctionInfo.ExecutionStage == ExecutionStage.Finished)
                {
                    executionTimespan = (runningFunctionInfo.EndTime - runningFunctionInfo.StartTime).TotalMilliseconds;
                }
                else
                {
                    executionTimespan = (currentTime - runningFunctionInfo.StartTime).TotalMilliseconds;
                }

                MetricsEventGenerator.LogFunctionExecutionEvent(
                    _executionId,
                    _appServiceOptions.CurrentValue.AppName,
                    concurrency,
                    runningFunctionInfo.Name,
                    runningFunctionInfo.InvocationId.ToString(),
                    runningFunctionInfo.ExecutionStage.ToString(),
                    (long)executionTimespan,
                    runningFunctionInfo.Success);

                if (_metricsPublisher != null)
                {
                    _metricsPublisher.AddFunctionExecutionActivity(
                        runningFunctionInfo.Name,
                        runningFunctionInfo.InvocationId.ToString(),
                        concurrency,
                        runningFunctionInfo.ExecutionStage.ToString(),
                        runningFunctionInfo.Success,
                        (long)executionTimespan,
                        _executionId,
                        currentTime,
                        runningFunctionInfo.StartTime);
                }

                if (_linuxContainerActivityPublisher != null)
                {
                    var activity = new ContainerFunctionExecutionActivity(DateTime.UtcNow, runningFunctionInfo.Name,
                        runningFunctionInfo.ExecutionStage, runningFunctionInfo.TriggerType,
                        runningFunctionInfo.Success);
                    _linuxContainerActivityPublisher.PublishFunctionExecutionActivity(activity);
                }
            }

            private static string GetDictionaryKey(string name, Guid invocationId)
            {
                return string.Format("{0}_{1}", name.ToString(), invocationId.ToString());
            }

            private void RaiseMetricsPerFunctionEvent()
            {
                List<FunctionMetrics> metricsEventsList = GetMetricsQueueSnapshot();

                var aggregatedEventsPerFunction = from item in metricsEventsList
                                                  group item by item.FunctionName into functionGroups
                                                  select new
                                                  {
                                                      FunctionName = functionGroups.Key,
                                                      StartedCount = Convert.ToUInt64(functionGroups.Count(x => x.ExecutionStage == ExecutionStage.Started)),
                                                      FailedCount = Convert.ToUInt64(functionGroups.Count(x => x.ExecutionStage == ExecutionStage.Failed)),
                                                      SucceededCount = Convert.ToUInt64(functionGroups.Count(x => x.ExecutionStage == ExecutionStage.Succeeded)),
                                                      TotalExectionTimeInMs = Convert.ToUInt64(functionGroups.Sum(x => Convert.ToDecimal(x.ExecutionTimeInMS)))
                                                  };

                foreach (var functionEvent in aggregatedEventsPerFunction)
                {
                    MetricsEventGenerator.LogFunctionExecutionAggregateEvent(_appServiceOptions.CurrentValue.AppName, functionEvent.FunctionName, (long)functionEvent.TotalExectionTimeInMs, (long)functionEvent.StartedCount, (long)functionEvent.SucceededCount, (long)functionEvent.FailedCount);
                }
            }

            private List<FunctionMetrics> GetMetricsQueueSnapshot()
            {
                var queueSnapshot = new List<FunctionMetrics>();
                var currentQueueLength = _functionMetricsQueue.Count;

                for (int iterator = 0; iterator < currentQueueLength; iterator++)
                {
                    if (_functionMetricsQueue.TryDequeue(out FunctionMetrics queueItem))
                    {
                        queueSnapshot.Add(queueItem);
                    }
                }

                return queueSnapshot;
            }

            private class RunningFunctionInfo
            {
                public RunningFunctionInfo(string name, Guid invocationId, DateTime startTime, bool success, string triggerType, ExecutionStage executionStage = ExecutionStage.InProgress)
                {
                    Name = name;
                    InvocationId = invocationId;
                    StartTime = startTime;
                    Success = success;
                    ExecutionStage = executionStage;
                    TriggerType = triggerType;
                }

                public string Name { get; private set; }

                public Guid InvocationId { get; private set; }

                public DateTime StartTime { get; private set; }

                public ExecutionStage ExecutionStage { get; set; }

                public DateTime EndTime { get; set; }

                public bool Success { get; set; }

                public string TriggerType { get; private set; }
            }
        }
    }
}