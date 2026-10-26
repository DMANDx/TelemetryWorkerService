using System;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace TelemetryWorkerService
{
    public class TelemetryPublisherWorker : BackgroundService
    {
        private readonly ILogger<TelemetryPublisherWorker> _logger;
        private readonly ConnectionFactory _connectionFactory; // <-- Добавляем фабрику
        private const string ExchangeName = "telemetry.topic";
        private const string RoutingKey = "metrics.apps.visualstudio";
        private IConnection? _connection;
        private IChannel? _channel;

        // Обновленный конструктор: принимает логгер и фабрику подключений
        public TelemetryPublisherWorker(ILogger<TelemetryPublisherWorker> logger, ConnectionFactory connectionFactory)
        {
            _logger = logger;
            _connectionFactory = connectionFactory;
        }

        public override async Task StartAsync(CancellationToken cancellationToken)
        {
            // Старый блок 'var factory = new ConnectionFactory() { ... }' удален!

            try
            {
                // Используем фабрику, переданную через DI
                _connection = await _connectionFactory.CreateConnectionAsync(cancellationToken);

                var channelOptions = new CreateChannelOptions(
                    publisherConfirmationsEnabled: true,
                    publisherConfirmationTrackingEnabled: true
                );

                _channel = await _connection.CreateChannelAsync(options: channelOptions, cancellationToken: cancellationToken);

                await _channel.ExchangeDeclareAsync(
                    exchange: ExchangeName,
                    type: ExchangeType.Topic,
                    durable: true,
                    cancellationToken: cancellationToken);

                _logger.LogInformation("Издатель телеметрии успешно инициализирован с V7 Publisher Confirms.");
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "КРИТИЧЕСКАЯ ОШИБКА: Издатель не смог подключиться к RabbitMQ!");
                throw;
            }

            await base.StartAsync(cancellationToken);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var currentProcess = Process.GetCurrentProcess();

            while (!stoppingToken.IsCancellationRequested)
            {
                if (_channel is null)
                {
                    _logger.LogError("Канал RabbitMQ не инициализирован. Публикация невозможна.");
                    return;
                }

                try
                {
                    currentProcess.Refresh();

                    var metric = new MetricMessage
                    {
                        ServiceName = "VS_Telemetry_Service",
                        InstanceId = Environment.MachineName,
                        CpuTimeMs = currentProcess.TotalProcessorTime.TotalMilliseconds,
                        MemoryUsageMb = Math.Round((double)currentProcess.WorkingSet64 / (1024 * 1024), 2),
                        ActiveThreads = currentProcess.Threads.Count,
                        Timestamp = DateTime.UtcNow
                    };

                    string json = JsonSerializer.Serialize(metric);
                    var body = Encoding.UTF8.GetBytes(json);
                    var properties = new BasicProperties { Persistent = false };

                    await _channel.BasicPublishAsync(
                        exchange: ExchangeName,
                        routingKey: RoutingKey,
                        mandatory: false,
                        basicProperties: properties,
                        body: body,
                        cancellationToken: stoppingToken);

                    if (_logger.IsEnabled(LogLevel.Information))
                    {
                        _logger.LogInformation("[Publisher] Отправлена живая метрика. RAM: {Memory} MB, Threads: {Threads}",
                            metric.MemoryUsageMb, metric.ActiveThreads);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError("[Publisher] Ошибка доставки! Брокер отключен или сокет упал. Ошибка: {Msg}", ex.Message);
                }

                await Task.Delay(1000, stoppingToken);
            }
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            if (_channel is not null) await _channel.CloseAsync(cancellationToken);
            if (_connection is not null) await _connection.CloseAsync(cancellationToken);
            await base.StopAsync(cancellationToken);
        }
    }
}
