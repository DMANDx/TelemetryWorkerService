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

public class MetricsCollectorWorkerTests : IAsyncLifetime
{
    // Описываем контейнер RabbitMQ для Testcontainers
    private readonly RabbitMqContainer _rabbitMqContainer = new RabbitMqBuilder()
        .WithImage("rabbitmq:3-management")
        .Build();

    // Автоматически запускаем Docker-контейнер перед тестами
    public async Task InitializeAsync() => await _rabbitMqContainer.StartAsync();

    // Автоматически удаляем контейнер после тестов
    public async Task DisposeAsync() => await _rabbitMqContainer.DisposeAsync();

    [Fact]
    public async Task Worker_ShouldConsumeAndAck_ValidTelemetryMessage()
    {
        // 1. Arrange: Получаем адрес из Docker и настраиваем тестовую фабрику
        var connectionString = _rabbitMqContainer.GetConnectionString();
        var factory = new ConnectionFactory { Uri = new Uri(connectionString) };

        using var cts = new CancellationTokenSource();

        // Создаем ваш реальный воркер, передавая ему Docker-фабрику
        var worker = new MetricsCollectorWorker(NullLogger<MetricsCollectorWorker>.Instance, factory);

        // Запускаем воркер, чтобы он создал структуру очередей в контейнере
        await worker.StartAsync(cts.Token);

        // Отдельное соединение для отправки тестового сообщения-мишени
        using var testConnection = await factory.CreateConnectionAsync(cts.Token);
        using var testChannel = await testConnection.CreateChannelAsync(cancellationToken: cts.Token);

        var testMetric = new MetricMessage
        {
            ServiceName = "Test_Source",
            InstanceId = "Test_PC",
            CpuTimeMs = 100,
            MemoryUsageMb = 200,
            ActiveThreads = 10,
            Timestamp = DateTime.UtcNow
        };

        string jsonMessage = JsonSerializer.Serialize(testMetric);
        var body = Encoding.UTF8.GetBytes(jsonMessage);

        // 2. Act: Публикуем сообщение в созданный воркером Exchange
        await testChannel.BasicPublishAsync(
            exchange: "telemetry.topic",
            routingKey: "metrics.apps.testapp",
            mandatory: true,
            body: body,
            cancellationToken: cts.Token);

        // 3. Assert: Ждем 3.5 секунды (в коде воркера стоит задержка обработки 3 секунды)
        await Task.Delay(3500);

        // Проверяем, что сообщение успешно обработалось и исчезло из очереди (кол-во сообщений = 0)
        var queueDeclareResult = await testChannel.QueueDeclarePassiveAsync("collector.metrics.vs", cts.Token);
        Assert.Equal(0u, queueDeclareResult.MessageCount);

        // Останавливаем воркер
        await worker.StopAsync(cts.Token);
    }

    [Fact]
    public async Task Worker_ShouldNackAndDiscard_InvalidMessage()
    {
        // Arrange
        var connectionString = _rabbitMqContainer.GetConnectionString();
        var factory = new ConnectionFactory { Uri = new Uri(connectionString) };

        using var cts = new CancellationTokenSource();
        var worker = new MetricsCollectorWorker(NullLogger<MetricsCollectorWorker>.Instance, factory);
        await worker.StartAsync(cts.Token);

        using var testConnection = await factory.CreateConnectionAsync(cts.Token);
        using var testChannel = await testConnection.CreateChannelAsync(cancellationToken: cts.Token);

        // Отправляем некорректный JSON (сломает десериализацию)
        var invalidBody = Encoding.UTF8.GetBytes("{ \"corrupted_json\": ");

        // Act
        await testChannel.BasicPublishAsync(
            exchange: "telemetry.topic",
            routingKey: "metrics.apps.corrupted",
            body: invalidBody,
            cancellationToken: cts.Token);

        await Task.Delay(1000); // Ошибка упадет сразу, 3 секунды ждать не нужно

        // Assert: Убеждаемся, что «битое» сообщение не застряло в очереди (Nack с requeue: false)
        var queueDeclareResult = await testChannel.QueueDeclarePassiveAsync("collector.metrics.vs", cts.Token);
        Assert.Equal(0u, queueDeclareResult.MessageCount);

        await worker.StopAsync(cts.Token);
    }
}
