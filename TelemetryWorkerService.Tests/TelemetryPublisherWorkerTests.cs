using System;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using TelemetryWorkerService;
using Testcontainers.RabbitMq;
using Xunit;


namespace TelemetryWorkerService.Tests;
//частично комментуирую пока
public class TelemetryPublisherWorkerTests : IAsyncLifetime
{
    // Поднимаем изолированный Docker-контейнер RabbitMQ для тестов издателя
    private readonly RabbitMqContainer _rabbitMqContainer = new RabbitMqBuilder()
        .WithImage("rabbitmq:3-management")
        .Build();

    public async Task InitializeAsync() => await _rabbitMqContainer.StartAsync();
    public async Task DisposeAsync() => await _rabbitMqContainer.DisposeAsync();

    [Fact]
    public async Task PublisherWorker_ShouldPublishValidMetricMessage_EverySecond()
    {
        // 1. Arrange: Подключаемся к Docker-контейнеру
        var connectionString = _rabbitMqContainer.GetConnectionString();
        var factory = new ConnectionFactory { Uri = new Uri(connectionString) };

        using var cts = new CancellationTokenSource();

        // Создаем тестовый канал, чтобы сделать "ловушку" для сообщений от воркера
        using var testConnection = await factory.CreateConnectionAsync(cts.Token);
        using var testChannel = await testConnection.CreateChannelAsync(cancellationToken: cts.Token);

        // Объявляем Exchange вручную, так как воркер при старте сделает то же самое
        await testChannel.ExchangeDeclareAsync("telemetry.topic", ExchangeType.Topic, durable: true, cancellationToken: cts.Token);

        // Создаем временную тестовую очередь и привязываем её к роутинг-ключу издателя ("metrics.apps.visualstudio")
        string testQueueName = "publisher.test.catcher.queue";
        await testChannel.QueueDeclareAsync(testQueueName, durable: false, exclusive: false, autoDelete: false, cancellationToken: cts.Token);
        await testChannel.QueueBindAsync(testQueueName, "telemetry.topic", "metrics.apps.visualstudio", cancellationToken: cts.Token);

        // Инициализируем наш реальный воркер-издатель
        var publisherWorker = new TelemetryPublisherWorker(NullLogger<TelemetryPublisherWorker>.Instance, factory);

        // 2. Act: Стартуем воркер. Внутри ExecuteAsync запустится цикл, который раз в секунду шлет метрики.
        await publisherWorker.StartAsync(cts.Token);

        // Ждем 1.5 секунды, чтобы воркер гарантированно успел сделать хотя бы одну публикацию
        await Task.Delay(1500);

        // 3. Assert: Вытаскиваем сообщение из нашей очереди-ловушки
        var result = await testChannel.BasicGetAsync(testQueueName, autoAck: true, cancellationToken: cts.Token);

        // Проверяем, что сообщение вообще долетело до брокера
        Assert.NotNull(result);

        // Десериализуем байты обратно в объект MetricMessage
        var jsonBytes = result.Body.ToArray();
        var jsonString = Encoding.UTF8.GetString(jsonBytes);
        var metric = JsonSerializer.Deserialize<MetricMessage>(jsonString);

        // Строгие проверки структуры данных: проверяем, что воркер заполнил поля реальной системной инфой
        Assert.NotNull(metric);
        Assert.Equal("VS_Telemetry_Service", metric.ServiceName);
        Assert.False(string.IsNullOrWhiteSpace(metric.InstanceId));
        Assert.True(metric.MemoryUsageMb > 0, "Показатель RAM должен быть больше нуля");
        Assert.True(metric.ActiveThreads > 0, "Количество активных потоков должно быть больше нуля");
        Assert.True(metric.Timestamp > DateTime.UtcNow.AddMinutes(-1), "Метка времени должна быть актуальной");

        // Останавливаем воркер после успешной проверки
        await publisherWorker.StopAsync(cts.Token);
    }
}
