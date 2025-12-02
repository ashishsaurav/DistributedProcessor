using Microsoft.Extensions.Logging;

namespace DistributedProcessor.Shared.Services
{
    public enum CircuitState { Closed, Open, HalfOpen }

    public interface ICircuitBreaker
    {
        CircuitState State { get; }
        Task<T> ExecuteAsync<T>(Func<Task<T>> action);
        Task ExecuteAsync(Func<Task> action);
        void Reset();
    }

    public class CircuitBreaker : ICircuitBreaker
    {
        private readonly int _failureThreshold;
        private readonly TimeSpan _openDuration;
        private readonly ILogger<CircuitBreaker> _logger;
        private readonly string _name;

        private CircuitState _state = CircuitState.Closed;
        private int _failureCount = 0;
        private DateTime _lastFailureTime;
        private DateTime _openedAt;
        private readonly SemaphoreSlim _lock = new(1, 1);

        public CircuitState State => _state;

        public CircuitBreaker(
            string name,
            ILogger<CircuitBreaker> logger,
            int failureThreshold = 5,
            int openDurationSeconds = 30)
        {
            _name = name;
            _logger = logger;
            _failureThreshold = failureThreshold;
            _openDuration = TimeSpan.FromSeconds(openDurationSeconds);
        }

        public async Task<T> ExecuteAsync<T>(Func<Task<T>> action)
        {
            await CheckStateAsync();

            try
            {
                var result = await action();
                await OnSuccessAsync();
                return result;
            }
            catch (Exception ex)
            {
                await OnFailureAsync(ex);
                throw;
            }
        }

        public async Task ExecuteAsync(Func<Task> action)
        {
            await ExecuteAsync(async () =>
            {
                await action();
                return true;
            });
        }

        private async Task CheckStateAsync()
        {
            await _lock.WaitAsync();
            try
            {
                if (_state == CircuitState.Open)
                {
                    if (DateTime.UtcNow - _openedAt >= _openDuration)
                    {
                        _state = CircuitState.HalfOpen;
                        _logger.LogInformation(
                            "Circuit {Name} transitioning to HalfOpen", _name);
                    }
                    else
                    {
                        throw new CircuitBreakerOpenException(
                            _name,
                            _openDuration - (DateTime.UtcNow - _openedAt));
                    }
                }
            }
            finally
            {
                _lock.Release();
            }
        }

        private async Task OnSuccessAsync()
        {
            await _lock.WaitAsync();
            try
            {
                if (_state == CircuitState.HalfOpen)
                {
                    _state = CircuitState.Closed;
                    _failureCount = 0;
                    _logger.LogInformation("Circuit {Name} closed after successful test", _name);
                }
                else if (_state == CircuitState.Closed)
                {
                    _failureCount = 0;
                }
            }
            finally
            {
                _lock.Release();
            }
        }

        private async Task OnFailureAsync(Exception ex)
        {
            await _lock.WaitAsync();
            try
            {
                _failureCount++;
                _lastFailureTime = DateTime.UtcNow;

                if (_state == CircuitState.HalfOpen)
                {
                    _state = CircuitState.Open;
                    _openedAt = DateTime.UtcNow;
                    _logger.LogWarning(
                        "Circuit {Name} reopened after failed test: {Error}",
                        _name, ex.Message);
                }
                else if (_state == CircuitState.Closed && _failureCount >= _failureThreshold)
                {
                    _state = CircuitState.Open;
                    _openedAt = DateTime.UtcNow;
                    _logger.LogWarning(
                        "Circuit {Name} opened after {Count} failures: {Error}",
                        _name, _failureCount, ex.Message);
                }
            }
            finally
            {
                _lock.Release();
            }
        }

        public void Reset()
        {
            _lock.Wait();
            try
            {
                _state = CircuitState.Closed;
                _failureCount = 0;
                _logger.LogInformation("Circuit {Name} manually reset", _name);
            }
            finally
            {
                _lock.Release();
            }
        }
    }

    public class CircuitBreakerOpenException : Exception
    {
        public string CircuitName { get; }
        public TimeSpan RetryAfter { get; }

        public CircuitBreakerOpenException(string circuitName, TimeSpan retryAfter)
            : base($"Circuit breaker '{circuitName}' is open. Retry after {retryAfter.TotalSeconds:F0}s")
        {
            CircuitName = circuitName;
            RetryAfter = retryAfter;
        }
    }
}
