using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace TelemetryWorkerService
{
    public class MetricsCollectorWorker : BackgroundService
    {
        private readonly ILogger<MetricsCollectorWorker> _logger;
        private readonly ConnectionFactory _connectionFactory; // Фабрика подключений приходит через DI

        // Константы основной топологии
        private const string ExchangeName = "telemetry.topic";
        private const string QueueName = "collector.metrics.vs";

        // Константы для Dead Letter Exchange (DLX)
        private const string DeadLetterExchangeName = "telemetry.dlx";
        private const string DeadLetterQueueName = "collector.metrics.vs.dead-letter";

        private IConnection? _connection;
        private IChannel? _channel;

        // Конструктор, принимающий зависимости через Dependency Injection
        public MetricsCollectorWorker(ILogger<MetricsCollectorWorker> logger, ConnectionFactory connectionFactory)
        {
            _logger = logger;
            _connectionFactory = connectionFactory;
        }

        public override async Task StartAsync(CancellationToken cancellationToken)
        {
            try
            {
                // Используем переданную фабрику (динамическую в тестах, localhost в продакшне)
                _connection = await _connectionFactory.CreateConnectionAsync(cancellationToken);
                _channel = await _connection.CreateChannelAsync(cancellationToken: cancellationToken);

                // 1. Настройка инфраструктуры для "мертвых" сообщений (DLX)
                await _channel.ExchangeDeclareAsync(exchange: DeadLetterExchangeName, type: ExchangeType.Fanout, durable: true, cancellationToken: cancellationToken);
                await _channel.QueueDeclareAsync(queue: DeadLetterQueueName, durable: true, exclusive: false, autoDelete: false, cancellationToken: cancellationToken);
                await _channel.QueueBindAsync(queue: DeadLetterQueueName, exchange: DeadLetterExchangeName, routingKey: string.Empty, cancellationToken: cancellationToken);

                // 2. Связывание основной очереди с DLX через аргументы конфигурации
                var queueArguments = new Dictionary<string, object?>
                {
                    { "x-dead-letter-exchange", DeadLetterExchangeName }
                };

                // 3. Настройка основной топологии
                await _channel.ExchangeDeclareAsync(exchange: ExchangeName, type: ExchangeType.Topic, durable: true, cancellationToken: cancellationToken);
                await _channel.QueueDeclareAsync(queue: QueueName, durable: true, exclusive: false, autoDelete: false, arguments: queueArguments, cancellationToken: cancellationToken);
                await _channel.QueueBindAsync(queue: QueueName, exchange: ExchangeName, routingKey: "metrics.apps.#", cancellationToken: cancellationToken);

                // Настройка Prefetch Count (Quality of Service)
                await _channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false, cancellationToken: cancellationToken);

                _logger.LogInformation("Сборщик телеметрии слушает queue: {Queue} с поддержкой DLX", QueueName);
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "КРИТИЧЕСКАЯ ОШИБКА: Подписчик не смог инициализироваться!");
                throw;
            }

            await base.StartAsync(cancellationToken);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            if (_channel is null)
            {
                _logger.LogError("Не удалось запустить подписчика: канал RabbitMQ не инициализирован.");
                return;
            }

            var consumer = new AsyncEventingBasicConsumer(_channel);

            consumer.ReceivedAsync += async (model, ea) =>
            {
                try
                {
                    var body = ea.Body.ToArray();
                    var json = Encoding.UTF8.GetString(body);
                    var metric = JsonSerializer.Deserialize<MetricMessage>(json);

                    if (metric is null)
                    {
                        _logger.LogError("Получено пустое сообщение телеметрии.");
                        // requeue: false при наличии x-dead-letter-exchange отправляет сообщение в DLX
                        await _channel!.BasicNackAsync(deliveryTag: ea.DeliveryTag, multiple: false, requeue: false, cancellationToken: stoppingToken);
                        return;
                    }

                    _logger.LogWarning("[Consumer Alert] Получены данные с ПК {Host}. Имитируем обработку 3 секунды...", metric.InstanceId);

                    await Task.Delay(3000, stoppingToken);

                    if (_logger.IsEnabled(LogLevel.Information))
                    {
                        _logger.LogInformation("Обработка метрики процесса ({Mem} MB) успешно завершена. Отправляем Ack.", metric.MemoryUsageMb);
                    }

                    await _channel!.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false, cancellationToken: stoppingToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Ошибка десериализации или обработки сообщения. Отправляем в DLX.");
                    // Любые поломки JSON или непредвиденные ошибки отправляют сообщение в DLQ (requeue: false)
                    await _channel!.BasicNackAsync(deliveryTag: ea.DeliveryTag, multiple: false, requeue: false, cancellationToken: stoppingToken);
                }
            };

            // Запускаем прослушивание очереди RabbitMQ
            await _channel.BasicConsumeAsync(
                queue: QueueName,
                autoAck: false,
                consumer: consumer,
                cancellationToken: stoppingToken);

            // Удерживаем метод ExecuteAsync активным, пока работает приложение
            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Delay(1000, stoppingToken);
            }
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            if (_channel != null) await _channel.CloseAsync(cancellationToken);
            if (_connection != null) await _connection.CloseAsync(cancellationToken);
            await base.StopAsync(cancellationToken);
        }
    }
}
