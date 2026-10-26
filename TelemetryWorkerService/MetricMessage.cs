using System;

namespace TelemetryWorkerService
{
    public class MetricMessage
    {
        public required string ServiceName { get; set; }
        public required string InstanceId { get; set; }
        public double CpuTimeMs { get; set; }
        public double MemoryUsageMb { get; set; }
        public int ActiveThreads { get; set; }
        public DateTime Timestamp { get; set; }
    }
}
