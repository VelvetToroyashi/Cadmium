using System.Buffers.Binary;
using System.Diagnostics;
using System.IO.Compression;
using System.Net;
using System.Net.WebSockets;
using System.Runtime.CompilerServices;
using System.Text.Json;
using CommunityToolkit.HighPerformance.Buffers;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Contrib.WaitAndRetry;
using Polly.Retry;
using Remora.Discord.API.Abstractions.Gateway;
using Remora.Discord.Gateway.Transport;
using Remora.Results;

namespace Cadmium.Dispatch.Gateway;

public class ZlibAwareWebSocketTransport : IPayloadTransportService
{
    private const int WebSocketSizeHint = 4096; // The gateway doesn't accept > 4kib.
    private const ushort ZLibMagicBytes = 0xDA70;
    private const WebSocketCloseStatus ReconnectingCloseStatus = (WebSocketCloseStatus)1012; // Service restart.
    
    private readonly MemoryStream _dataStream;
    private readonly JsonSerializerOptions _serializerOptions;
    private readonly AsyncRetryPolicy<ClientWebSocket> _clientRetry;
    
    private ClientWebSocket? _webSocket;

    public ZlibAwareWebSocketTransport(IOptionsMonitor<JsonSerializerOptions> jsonOptions)
    {
        _serializerOptions = jsonOptions.Get("Discord");

        _dataStream = new MemoryStream();
        _clientRetry = Policy<ClientWebSocket>.Handle<WebSocketException>(HandleWebSocketRetry)
                                              .WaitAndRetryAsync(Backoff.DecorrelatedJitterBackoffV2(TimeSpan.FromSeconds(1), 5));
    }

    public bool IsConnected => _webSocket?.State is WebSocketState.Open;

    public async Task<Result> ConnectAsync(Uri endpoint, CancellationToken ct = default)
    {
        if (IsConnected)
        {
            return new InvalidOperationError("The websocket is already connected.");
        }
        
        try
        {
            var socket = _webSocket = await _clientRetry.ExecuteAsync
            (
                static c => CreateAndConnectSocketAsync((Uri)c["endpoint"]),
                new Context { { "endpoint", endpoint } }
            );

            if (socket.State is not (WebSocketState.Open or WebSocketState.Connecting))
            {
                // Polly should eventually throw a circuit-breaker exception.
                throw new UnreachableException("Polly did not handle the Websocket failure. This is likely a bug.");
            }
        }
        catch (Exception e)
        {
            return e;
        }
        
        return Result.FromSuccess();
    }

    public async Task<Result> DisconnectAsync(bool reconnectionIntended, CancellationToken ct = default)
    {
        if (_webSocket?.State is WebSocketState.None or WebSocketState.Aborted or WebSocketState.Closed)
        {
            return new InvalidOperationError("The websocket was not initialized or connected.");
        }

        var closeCode = reconnectionIntended ? ReconnectingCloseStatus : WebSocketCloseStatus.NormalClosure;

        try
        {
            await _webSocket!.CloseAsync(closeCode, null, CancellationToken.None);
        }
        catch (WebSocketException)
        {
            // Ignore.
        }
        
        _webSocket!.Dispose();
        _webSocket = null;
        
        return Result.FromSuccess();
    }

    Task<Result> IPayloadTransportService.SendPayloadAsync(IPayload payload, CancellationToken ct) => SendPayloadAsync(payload, ct).AsTask();

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<Result> SendPayloadAsync(IPayload payload, CancellationToken ct = default)
    {
        if (!IsConnected)
        {
            return new InvalidOperationError("The websocket is not open or initialized.");
        }

        // PERF: Allocate our own writer to save bytes (160 vs 920 in testing)
        using var bufferWriter = new ArrayPoolBufferWriter<byte>(WebSocketSizeHint);
        await using var writer = new Utf8JsonWriter(bufferWriter);
        
        JsonSerializer.Serialize(writer, payload, _serializerOptions);
        var bytes = bufferWriter.DangerousGetArray().Array!;
        
        if (bytes.Length > WebSocketSizeHint)
        {
            return new InvalidOperationError($"The payload is too large (Max: {WebSocketSizeHint} bytes. Got: {bytes.Length} bytes).");
        }

        try
        {
            await _webSocket!.SendAsync(new ArraySegment<byte>(bytes), WebSocketMessageType.Text, true, CancellationToken.None);
        }
        catch (Exception e)
        {
            return e;
        }
        
        return Result.FromSuccess();
    }

    Task<Result<IPayload>> IPayloadTransportService.ReceivePayloadAsync(CancellationToken ct) => ReceivePayloadAsync(ct).AsTask();

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<Result<IPayload>> ReceivePayloadAsync(CancellationToken ct = default)
    {
        if (_webSocket?.State is not WebSocketState.Open)
        {
            return new InvalidOperationError("The websocket is not open or initialized.");
        }

        using var bufferWriter = new ArrayPoolBufferWriter<byte>();

        ValueWebSocketReceiveResult result;

        do
        {
            var memory = bufferWriter.GetMemory(WebSocketSizeHint);
            result = await _webSocket!.ReceiveAsync(memory, CancellationToken.None);

            bufferWriter.Advance(result.Count);
        } while (!result.EndOfMessage);

        return HandlePayload();
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        Result<IPayload> HandlePayload()
        {
            ReadOnlySpan<byte> resultingBytes;

            if (BinaryPrimitives.ReadUInt16LittleEndian(bufferWriter.WrittenSpan[..2]) is not ZLibMagicBytes)
            {
                // This payload isn't compressed, pass it along:
                resultingBytes = bufferWriter.WrittenSpan;
            }
            else
            {
                using var compressionStream = new MemoryStream(bufferWriter.DangerousGetArray().Array!);
                using var deflateStream = new DeflateStream(compressionStream, CompressionMode.Decompress);

                // ReSharper disable once MethodHasAsyncOverloadWithCancellation
                deflateStream.CopyTo(_dataStream);
                resultingBytes = _dataStream.GetBuffer();
                _dataStream.Position = 0;
            }

            var payload = JsonSerializer.Deserialize<IPayload>(resultingBytes, _serializerOptions)!;
            return Result<IPayload>.FromSuccess(payload);
        }
    }


    private static async Task<ClientWebSocket> CreateAndConnectSocketAsync(Uri uri)
    {
        var client = new ClientWebSocket();
        await client.ConnectAsync(uri, CancellationToken.None);

        return client;
    }
    
    private bool HandleWebSocketRetry(WebSocketException wex)
    {
        if (wex.InnerException is HttpRequestException)
        {
            return true;
        }
        
        return wex.WebSocketErrorCode switch
        {
            WebSocketError.Faulted => true,
            WebSocketError.HeaderError => true,
            WebSocketError.NativeError => true,
            WebSocketError.NotAWebSocket when (int)_webSocket!.HttpStatusCode is > 499 or (int)HttpStatusCode.RequestTimeout => true,
            _ => false,
        };
    }
}