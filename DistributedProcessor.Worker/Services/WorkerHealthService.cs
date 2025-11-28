using System;
using System.Net.Http;
using System.Net.Http.Json;
using System.Threading.Tasks;
using DistributedProcessor.Shared.Models;
using Microsoft.Extensions.Logging;

namespace DistributedProcessor.Worker.Services
{
    public interface IWorkerHealthService
    {
        Task ReportHealthAsync(WorkerStatus status);
    }

    public class WorkerHealthService : IWorkerHealthService
    {
        private readonly HttpClient _httpClient;
        private readonly ILogger<WorkerHealthService> _logger;
        private readonly string _healthEndpointUrl;

        public WorkerHealthService(ILogger<WorkerHealthService> logger)
        {
            _logger = logger;
            _httpClient = new HttpClient();
            // Set your API health endpoint URL here
            _healthEndpointUrl = "http://localhost:5000/api/health/update";
        }

        public async Task ReportHealthAsync(WorkerStatus status)
        {
            try
            {
                var response = await _httpClient.PostAsJsonAsync(_healthEndpointUrl, status);
                response.EnsureSuccessStatusCode();
                _logger.LogDebug($"Sent heartbeat for worker {status.WorkerId} with state {status.State}");
            }
            catch (Exception ex)
            {
                _logger.LogWarning($"Failed to send heartbeat: {ex.Message}");
            }
        }
    }
}
