using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using System;
using System.Threading.Tasks;

namespace TelemetryWorkerService
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("=== ЗАПУСК СЕРВИСА ТЕЛЕМЕТРИИ ===");

            var host = Host.CreateDefaultBuilder(args)
                .ConfigureServices((hostContext, services) =>
                {
                    // Регистрируем фабрику подключений для обычного запуска приложения на localhost
                    services.AddSingleton(new ConnectionFactory()
                    {
                        HostName = "localhost",
                        AutomaticRecoveryEnabled = true,
                        NetworkRecoveryInterval = TimeSpan.FromSeconds(5),
                        RequestedConnectionTimeout = TimeSpan.FromSeconds(3),
                        ContinuationTimeout = TimeSpan.FromSeconds(3),
                        RequestedHeartbeat = TimeSpan.FromSeconds(5)
                    });

                    services.AddHostedService<TelemetryPublisherWorker>();
                    services.AddHostedService<MetricsCollectorWorker>();
                })
                .Build();

            await host.RunAsync();
        }
    }
}
