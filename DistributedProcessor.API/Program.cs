using DistributedProcessor.API.Hubs;
using DistributedProcessor.API.Services;
using DistributedProcessor.Data;
using DistributedProcessor.Data.Services;
using Microsoft.EntityFrameworkCore;
using Serilog;

var builder = WebApplication.CreateBuilder(args);

Log.Logger = new LoggerConfiguration()
    .WriteTo.Console()
    .WriteTo.File("logs/api-.log", rollingInterval: RollingInterval.Day)
    .CreateLogger();

builder.Host.UseSerilog();

builder.Services.AddDbContext<ApplicationDbContext>(options =>
    options.UseSqlServer(builder.Configuration.GetConnectionString("DefaultConnection")));

builder.Services.AddMemoryCache();
builder.Services.AddControllers();
builder.Services.AddSignalR();

builder.Services.AddSingleton<IKafkaAdminService, KafkaAdminService>();
builder.Services.AddScoped<ITaskOrchestrator, TaskOrchestrator>();
builder.Services.AddScoped<IDbService, DbService>();
builder.Services.AddSingleton<IWorkerHealthService, WorkerHealthService>();
builder.Services.AddScoped<ISimpleJobOrchestrator, SimpleJobOrchestrator>();
builder.Services.AddScoped<IDashboardService, DashboardService>();
builder.Services.AddSingleton<ISmartTaskDispatcher, SmartTaskDispatcher>();
builder.Services.AddHostedService<DashboardUpdateService>();
builder.Services.AddHostedService<JobStatusUpdateService>();
builder.Services.AddSingleton<ICollectorHealthService, CollectorHealthService>();
builder.Services.AddScoped<IDeadLetterService, DeadLetterService>();
builder.Services.AddScoped<IIdempotencyService, IdempotencyService>();
builder.Services.AddHostedService<IdempotencyCleanupService>();

builder.Services.AddCors(options =>
{
    options.AddPolicy("AllowWebUI", policy =>
    {
        policy.WithOrigins(
            "http://localhost:5249",   // Web UI HTTP
            "https://localhost:7230"   // Web UI HTTPS
        )
        .AllowAnyHeader()
        .AllowAnyMethod()
        .AllowCredentials();  // CRITICAL for SignalR
    });
});

var app = builder.Build();

using var scope = app.Services.CreateScope();
var kafkaAdmin = scope.ServiceProvider.GetRequiredService<IKafkaAdminService>();
await kafkaAdmin.CreateTopicsAsync();

// Uncomment when ready
//var db = scope.ServiceProvider.GetRequiredService<ApplicationDbContext>();
//await db.Database.MigrateAsync();

app.UseCors("AllowWebUI");  // Must come before UseRouting

app.UseRouting();
app.MapControllers();
app.MapHub<MonitoringHub>("/hubs/monitoring");

app.Run();
