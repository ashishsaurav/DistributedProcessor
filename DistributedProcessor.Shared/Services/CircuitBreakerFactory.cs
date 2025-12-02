using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace DistributedProcessor.Shared.Services
{
    public interface ICircuitBreakerFactory
    {
        ICircuitBreaker GetOrCreate(string name);
        Dictionary<string, CircuitState> GetAllStates();
        void ResetAll();
    }

    public class CircuitBreakerFactory : ICircuitBreakerFactory
    {
        private readonly ConcurrentDictionary<string, ICircuitBreaker> _circuits = new();
        private readonly ILoggerFactory _loggerFactory;

        public CircuitBreakerFactory(ILoggerFactory loggerFactory)
        {
            _loggerFactory = loggerFactory;
        }

        public ICircuitBreaker GetOrCreate(string name)
        {
            return _circuits.GetOrAdd(name, n => new CircuitBreaker(
                n,
                _loggerFactory.CreateLogger<CircuitBreaker>(),
                failureThreshold: 5,
                openDurationSeconds: 30));
        }

        public Dictionary<string, CircuitState> GetAllStates()
        {
            return _circuits.ToDictionary(
                kvp => kvp.Key,
                kvp => kvp.Value.State);
        }

        public void ResetAll()
        {
            foreach (var circuit in _circuits.Values)
            {
                circuit.Reset();
            }
        }
    }
}
