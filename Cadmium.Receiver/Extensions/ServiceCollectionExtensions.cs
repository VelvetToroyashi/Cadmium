using System.Text.Json;
using Cadmium.Receiver.Services;
using MassTransit;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using Remora.Discord.Gateway;
using Remora.Discord.Gateway.Extensions;

namespace Cadmium.Receiver.Extensions;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddCadmium
    (
        this IServiceCollection services,
        Func<IServiceProvider, string> rabbitMqUrlFactory,
        Func<IServiceProvider, string> tokenFactory,
        Action<IHttpClientBuilder>? buildClient = null
    )
    {
        services.AddDiscordGateway(tokenFactory, buildClient);
        services.Remove(ServiceDescriptor.Describe(typeof(DiscordGatewayClient), typeof(DiscordGatewayClient), ServiceLifetime.Singleton));

        services.AddMassTransit
        (
            bus =>
            {
                bus.UsingRabbitMq(Configure);
                bus.AddConsumer<CadmiumDispatcher>();
            }
        );

        return services;

        void Configure(IBusRegistrationContext ctx, IRabbitMqBusFactoryConfigurator rmq)
        {
            var connString = rabbitMqUrlFactory(ctx);
            
            if (string.IsNullOrEmpty(connString))
            {
                throw new InvalidOperationException("RabbitMQ connection string was not present in the configuration.");
            }
            
            rmq.ConfigureEndpoints(ctx);
            rmq.Host(new Uri(connString));
            rmq.ExchangeType = ExchangeType.Direct;
            rmq.Durable = true;
            rmq.ConfigureJsonSerializerOptions(_ => ctx.GetRequiredService<IOptionsMonitor<JsonSerializerOptions>>().Get("Discord"));
        }
    }
    
}