using System.Collections.Concurrent;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using HealthStatus = Elastic.Clients.Elasticsearch.HealthStatus;

namespace HealthChecks.Elasticsearch;

public class ElasticsearchHealthCheck : IHealthCheck
{
    private static readonly ConcurrentDictionary<string, ElasticsearchClient> _connections = new();

    private readonly ElasticsearchOptions _options;

    public ElasticsearchHealthCheck(ElasticsearchOptions options)
    {
        _options = Guard.ThrowIfNull(options);
    }

    /// <inheritdoc />
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            if (!_connections.TryGetValue(_options.Uri, out var lowLevelClient))
            {
                var settings = new ElasticsearchClientSettings(new Uri(_options.Uri));

                if (_options.RequestTimeout.HasValue)
                {
                    settings = settings.RequestTimeout(_options.RequestTimeout.Value);
                }

                if (_options is {AuthenticateWithBasicCredentials: true, UserName: not null, Password: not null})
                {
                    settings = settings.Authentication(new BasicAuthentication(_options.UserName, _options.Password));
                }
                else if (_options is {AuthenticateWithCertificate: true, Certificate: not null})
                {
                    settings = settings.ClientCertificate(_options.Certificate);
                }
                else if (_options is {AuthenticateWithApiKey: true, ApiKey: not null})
                {
                    settings = settings.Authentication(new ApiKey(_options.ApiKey));
                }

                if (_options.CertificateValidationCallback != null)
                {
                    settings = settings.ServerCertificateValidationCallback(_options.CertificateValidationCallback);
                }

                lowLevelClient = new ElasticsearchClient(settings);

                if (!_connections.TryAdd(_options.Uri, lowLevelClient))
                {
                    lowLevelClient = _connections[_options.Uri];
                }
            }

            if (_options.UseClusterHealthApi)
            {
                var healthResponse = await lowLevelClient.Cluster.HealthAsync(cancellationToken).ConfigureAwait(false);

                if (healthResponse.ApiCallDetails.HttpStatusCode != 200)
                {
                    return new HealthCheckResult(context.Registration.FailureStatus);
                }

                return healthResponse.Status switch
                {
                    HealthStatus.Green => HealthCheckResult.Healthy(),
                    HealthStatus.Yellow => HealthCheckResult.Degraded(),
                    _ => new HealthCheckResult(context.Registration.FailureStatus)
                };
            }

            var pingResult = await lowLevelClient.PingAsync(cancellationToken).ConfigureAwait(false);
            bool isSuccess = pingResult.ApiCallDetails.HttpStatusCode == 200;

            return isSuccess
                ? HealthCheckResult.Healthy()
                : new HealthCheckResult(context.Registration.FailureStatus);
        }
        catch (Exception ex)
        {
            return new HealthCheckResult(context.Registration.FailureStatus, exception: ex);
        }
    }
}
