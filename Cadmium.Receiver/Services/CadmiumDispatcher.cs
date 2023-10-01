using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using MassTransit;
using Microsoft.Extensions.Logging;
using Remora.Discord.API;
using Remora.Discord.API.Abstractions.Gateway;
using Remora.Discord.API.Abstractions.Gateway.Events;
using Remora.Discord.Gateway.Services;
using BindingFlags = System.Reflection.BindingFlags;

namespace Cadmium.Receiver.Services;

public class CadmiumDispatcher(IResponderDispatchService _dispatch, ILogger<CadmiumDispatcher> _logger) : IConsumer<IGatewayEvent>
{
    public async Task Consume(ConsumeContext<IGatewayEvent> context)
    {
        if (_logger.IsEnabled(LogLevel.Trace))
        {
            _logger.LogTrace("Received payload: {Payload}", context.Message.GetType().Name);
        }

        var payload = PayloadFixer.ConstructDispatchPayload(context.Message);

        var dispatchResult = await _dispatch.DispatchAsync(payload);

        if (!dispatchResult.IsSuccess)
        {
            _logger.LogWarning("Failed to dispatch a payload ({PayloadType})", context.Message.GetType().Name);
        }
    }
}

file static class PayloadFixer
{
    private static readonly HashSet<Type> _knownTypes = new();
    private static readonly SemaphoreSlim _constructionLock = new(1, 1);
    private static readonly Dictionary<Type, Func<IGatewayEvent, IPayload>> _converterCache = new();

    private static readonly Type _genericPayload = typeof(Payload<>);
    private static readonly Type _delegateType = typeof(Func<IGatewayEvent, IPayload>);

    public static IPayload ConstructDispatchPayload(IGatewayEvent payload)
    {
        var payloadType = payload.GetType();

        if (_knownTypes.Contains(payloadType))
        {
            return _converterCache[payloadType](payload);
        }

        if (_constructionLock.CurrentCount is 0) // We're already constructing a delegate, so, wait
        {
            _constructionLock.Wait();
            return _converterCache[payloadType](payload);
        }

        CreatePayloadDelegate(payloadType);
        return _converterCache[payloadType](payload);
    }

    private static void CreatePayloadDelegate(Type payloadType)
    {
        var argument = Expression.Parameter(payloadType, "payload");
        var ctor = _genericPayload.MakeGenericType(payloadType).GetConstructor(BindingFlags.Public, new[] { payloadType })!;

        var instance = Expression.New(ctor, argument);

        var cast = Expression.Convert(instance, typeof(IPayload));

        var lambda = Expression.Lambda(_delegateType, Expression.Block(instance, cast), argument).Compile();

        _converterCache[payloadType] = Unsafe.As<Func<IGatewayEvent, IPayload>>(lambda);
        _knownTypes.Add(payloadType);
    }
}