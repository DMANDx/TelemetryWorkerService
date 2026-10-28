using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Xunit;
using RabbitMQ.Client;
using TelemetryWorkerService;
using Testcontainers.RabbitMq;
using Testcontainers.Toxiproxy;
using DotNet.Testcontainers.Builders;
using Xunit;
using Xunit.Abstractions;

namespace TelemetryWorkerService.Tests
{ 
public class AdvancedRabbitMqTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _output;
    private readonly ILoggerFactory _loggerFactory;

    // Контейнеры инфраструктуры
    private readonly RabbitMqContainer _rabbitMqContainer;
    private readonly ToxiproxyContainer _toxiproxyContainer;

        [Obsolete]
        public AdvancedRabbitMqTests(ITestOutputHelper output)
    {
        _output = output;

        // Настройка провайдера логирования
        _loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddProvider(new XunitLoggerProvider(output));
        });

        // актуальный NetworkBuilder вместо старого DockerNetworkBuilder
        var network = new NetworkBuilder().Build();

        _rabbitMqContainer = new RabbitMqBuilder()
            .WithImage("rabbitmq:3-management")
            .WithNetwork(network)
            .Build();

        _toxiproxyContainer = new ToxiproxyBuilder()
            .WithImage("ghcr.io/shopify/toxiproxy:2.5.0")
            .WithNetwork(network)
            .Build();
    }

    public async Task InitializeAsync()
    {      
        await _rabbitMqContainer.StartAsync();
        await _toxiproxyContainer.StartAsync();
    }

    public async Task DisposeAsync()
    {        
        await _rabbitMqContainer.DisposeAsync();
        await _toxiproxyContainer.DisposeAsync();
        _loggerFactory.Dispose();
    }

    [Fact]
    public async Task DLX_Scenario_ShouldForwardCorruptedMessageToDeadLetterQueue()
    {
        _output.WriteLine("=== СТАРТ ТЕСТА: ПРОВЕРКА DLX НА ПЛОХИХ ДАННЫХ ===");

        var connectionString = _rabbitMqContainer.GetConnectionString();
        var factory = new ConnectionFactory { Uri = new Uri(connectionString) };
        using var cts = new CancellationTokenSource();

        var workerLogger = _loggerFactory.CreateLogger<MetricsCollectorWorker>();
        var worker = new MetricsCollectorWorker(workerLogger, factory);
        await worker.StartAsync(cts.Token);

        using var testConnection = await factory.CreateConnectionAsync(cts.Token);
        using var testChannel = await testConnection.CreateChannelAsync(cancellationToken: cts.Token);

        var brokenBody = Encoding.UTF8.GetBytes("ЭТО_НЕ_JSON_СТРУКТУРА_А_ПРОСТО_ТЕКСТ");

        _output.WriteLine("Публикуем битое сообщение в топик...");
        await testChannel.BasicPublishAsync(
            exchange: "telemetry.topic",
            routingKey: "metrics.apps.malformed",
            body: brokenBody,
            cancellationToken: cts.Token);

        await Task.Delay(1500);

        var mainQueue = await testChannel.QueueDeclarePassiveAsync("collector.metrics.vs", cts.Token);
        Assert.Equal(0u, mainQueue.MessageCount);

        var dlQueue = await testChannel.QueueDeclarePassiveAsync("collector.metrics.vs.dead-letter", cts.Token);
        _output.WriteLine($"Сообщений в очереди мертвого письма (DLQ): {dlQueue.MessageCount}");

        Assert.Equal(1u, dlQueue.MessageCount);

        await worker.StopAsync(cts.Token);
    }
    [Fact]
    public async Task NetworkPartition_Scenario_ClientShouldRecover_AfterConnectionRestored()
    {
        _output.WriteLine("=== СТАРТ ТЕСТА: ТЕСТИРОВАНИЕ ОБРЫВА СЕТИ И АВТО-ВОССТАНОВЛЕНИЯ ===");

        // 1. Получаем прокси-строку подключения напрямую из контейнера Toxiproxy
        var proxyConnectionString = _toxiproxyContainer.GetConnectionString();

        var factory = new ConnectionFactory
        {
            Uri = new Uri(proxyConnectionString),
            AutomaticRecoveryEnabled = true,
            NetworkRecoveryInterval = TimeSpan.FromSeconds(1)
        };

        var workerLogger = _loggerFactory.CreateLogger<MetricsCollectorWorker>();
        var worker = new MetricsCollectorWorker(workerLogger, factory);

        _output.WriteLine("Запускаем воркер через сетевой прокси...");
        await worker.StartAsync(CancellationToken.None);

        // Проверяем, что соединение стабильно работает до разрыва
        using var testConnection = await factory.CreateConnectionAsync();
        using var testChannel = await testConnection.CreateChannelAsync();
        var mainQueue = await testChannel.QueueDeclarePassiveAsync("collector.metrics.vs");
        Assert.Equal(0u, mainQueue.MessageCount);

        // 2. Для отключения сети используем встроенные HttpClient-команды Toxiproxy
        _output.WriteLine("!!! АВАРИЯ: Разрываем соединение в Toxiproxy (сеть упала) !!!");

        using (var client = new System.Net.Http.HttpClient())
        {            
            var proxAddr = _toxiproxyContainer.Hostname + ":" + _toxiproxyContainer.GetMappedPublicPort(8474);

            // Отправляем команду на отключение прокси-линка брокера
            await client.PostAsync($"http://{proxAddr}/proxies/rabbitmq/disable", null);
        }

        // Пробуем сделать запрос во время аварии — должно упасть по таймауту/дисконнекту
        await Assert.ThrowsAnyAsync<Exception>(async () => {
            await testChannel.QueueDeclarePassiveAsync("collector.metrics.vs");
        });
        _output.WriteLine("Проверка подтвердила: связь с RabbitMQ полностью потеряна.");

        await Task.Delay(2000);

        // 3. Включаем сеть обратно через HTTP API
        _output.WriteLine("=== РЕМОНТ: Сеть восстановлена. Ожидаем Авто-Восстановление RabbitMQ.Client ===");
        using (var client = new System.Net.Http.HttpClient())
        {            
            var proxAddr = _toxiproxyContainer.Hostname + ":" + _toxiproxyContainer.GetMappedPublicPort(8474);

            // Отправляем команду на включение прокси-линка обратно
            await client.PostAsync($"http://{proxAddr}/proxies/rabbitmq/enable", null);
            await client.PostAsync($"http://{proxAddr}/proxies/rabbitmq/enable", null);
        }

        // Даем драйверу RabbitMQ.Client время обнаружить сеть и переподключить внутренние сокеты
        await Task.Delay(3000);

        // Проверяем: Успешный пассивный запрос к очереди докажет, что воркер и наш тестовый канал восстановились
        var recoveredQueue = await testChannel.QueueDeclarePassiveAsync("collector.metrics.vs");
        _output.WriteLine("Сеть поднялась, сокеты восстановили работу автоматически!");

        Assert.Equal(0u, recoveredQueue.MessageCount);

        await worker.StopAsync(CancellationToken.None);
    }


}
}