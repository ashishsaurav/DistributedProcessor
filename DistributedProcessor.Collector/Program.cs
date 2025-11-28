using DistributedProcessor.Collector.Services;
using DistributedProcessor.Data;
using DistributedProcessor.Data.Services;
using Microsoft.EntityFrameworkCore;
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
    services.AddDbContext<ApplicationDbContext>(options =>
        options.UseSqlServer(hostContext.Configuration.GetConnectionString("DefaultConnection")));

    services.AddScoped<IDbService, DbService>();
    services.AddHostedService<CollectorService>();
});

await builder.RunConsoleAsync();
