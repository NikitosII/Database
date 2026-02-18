using Database.Core;
using Database.Core.Interfaces;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Database.Server.Services
{
    public class DatabaseMaintenanceService : BackgroundService
    {
        private readonly ICowDatabase _database;
        private readonly ILogger<DatabaseMaintenanceService> _logger;
        private readonly TimeSpan _maintenanceInterval = TimeSpan.FromMinutes(5);

        public DatabaseMaintenanceService(ICowDatabase database, ILogger<DatabaseMaintenanceService> logger)
        {
            _database = database;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(_maintenanceInterval, stoppingToken);
                    _logger.LogInformation("Database maintenance tick (no-op for now)");
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Maintenance task failed");
                }
            }
        }
    }
}
