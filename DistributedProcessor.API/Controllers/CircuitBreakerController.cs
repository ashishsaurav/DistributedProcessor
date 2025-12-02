using DistributedProcessor.Shared.Services;
using Microsoft.AspNetCore.Mvc;

namespace DistributedProcessor.API.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class CircuitBreakerController : ControllerBase
    {
        private readonly ICircuitBreakerFactory _circuitBreakerFactory;

        public CircuitBreakerController(ICircuitBreakerFactory circuitBreakerFactory)
        {
            _circuitBreakerFactory = circuitBreakerFactory;
        }

        [HttpGet("status")]
        public IActionResult GetStatus()
        {
            var states = _circuitBreakerFactory.GetAllStates();
            return Ok(new
            {
                timestamp = DateTime.UtcNow,
                circuits = states.Select(kvp => new
                {
                    name = kvp.Key,
                    state = kvp.Value.ToString(),
                    isHealthy = kvp.Value == CircuitState.Closed
                })
            });
        }

        [HttpPost("reset")]
        public IActionResult ResetAll()
        {
            _circuitBreakerFactory.ResetAll();
            return Ok(new { message = "All circuit breakers reset" });
        }

        [HttpPost("reset/{name}")]
        public IActionResult Reset(string name)
        {
            var circuit = _circuitBreakerFactory.GetOrCreate(name);
            circuit.Reset();
            return Ok(new { message = $"Circuit breaker '{name}' reset" });
        }
    }
}
