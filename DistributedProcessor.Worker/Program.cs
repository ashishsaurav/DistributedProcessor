using DistributedProcessor.Worker.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;

var builder = Host.CreateDefaultBuilder(args);

builder.UseSerilog((context, services, configuration) =>
{
    configuration.ReadFrom.Configuration(context.Configuration)
                 .ReadFrom.Services(services)
                 .Enrich.FromLogContext()
                 .WriteTo.Console();
});

builder.ConfigureServices((hostContext, services) =>
{
    services.AddHostedService<WorkerService>();
    services.AddSingleton<IWorkerHealthService, WorkerHealthService>();
});

await builder.RunConsoleAsync();
