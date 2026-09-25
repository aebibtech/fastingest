using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using FastIngest.Core.Results;

namespace FastIngest.Core.Readers;

/// <summary>
/// Represents the parsed result of a single line from a JSON Lines (NDJSON) stream.
/// </summary>
/// <typeparam name="T">The record type.</typeparam>
/// <param name="LineNumber">The 1-based line number in the source stream.</param>
/// <param name="Record">The deserialized record instance, or default if parsing failed.</param>
/// <param name="Error">The captured row error if parsing or schema validation failed, or null if successful.</param>
/// <param name="IsSuccess">True if the line was successfully parsed and deserialized; otherwise false.</param>
internal readonly record struct JsonLineItem<T>(long LineNumber, T? Record, IngestRowError? Error, bool IsSuccess)
{
    /// <summary>
    /// Creates a successful line item result.
    /// </summary>
    public static JsonLineItem<T> Success(long lineNumber, T record) => new(lineNumber, record, null, true);

    /// <summary>
    /// Creates a failed line item result with error details.
    /// </summary>
    public static JsonLineItem<T> Failure(long lineNumber, IngestRowError error) => new(lineNumber, default, error, false);
}

/// <summary>
/// Static helper methods for reading line-delimited JSON (NDJSON / JSONL) streams.
/// </summary>
internal static class JsonLinesStreamReader
{
    /// <summary>
    /// Asynchronously streams deserialized records of type <typeparamref name="TRecord"/> from the provided stream.
    /// </summary>
    /// <typeparam name="TRecord">The model type representing each JSON line record.</typeparam>
    /// <param name="stream">The readable stream containing line-delimited JSON.</param>
    /// <param name="options">Optional JSON serializer options.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>An async enumerable yielding deserialized records.</returns>
    public static IAsyncEnumerable<TRecord> ReadAsync<TRecord>(
        Stream stream,
        JsonSerializerOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var reader = new JsonLinesStreamReader<TRecord>(stream, options, cancellationToken);
        return reader.ReadAsync(cancellationToken);
    }

    /// <summary>
    /// Asynchronously streams line item results (including error details) for each record in the stream.
    /// </summary>
    /// <typeparam name="TRecord">The model type representing each JSON line record.</typeparam>
    /// <param name="stream">The readable stream containing line-delimited JSON.</param>
    /// <param name="options">Optional JSON serializer options.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>An async enumerable yielding <see cref="JsonLineItem{TRecord}"/> items.</returns>
    public static IAsyncEnumerable<JsonLineItem<TRecord>> ReadLineItemsAsync<TRecord>(
        Stream stream,
        JsonSerializerOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var reader = new JsonLinesStreamReader<TRecord>(stream, options, cancellationToken);
        return reader.ReadLineItemsAsync(cancellationToken);
    }
}

/// <summary>
/// High-throughput, constant-memory streaming reader for line-delimited JSON (JSON Lines / NDJSON).
/// Streams records asynchronously without deserializing the entire document into an in-memory collection.
/// </summary>
/// <typeparam name="TRecord">The strongly-typed model representing an ingested record.</typeparam>
internal sealed class JsonLinesStreamReader<TRecord> : IAsyncEnumerable<TRecord>
{
    private static readonly JsonSerializerOptions DefaultSerializerOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    private readonly PipeReader _pipeReader;
    private readonly bool _ownsPipeReader;
    private readonly JsonSerializerOptions _options;
    private readonly CancellationToken _defaultCancellationToken;

    /// <summary>
    /// Initializes a new instance of the <see cref="JsonLinesStreamReader{TRecord}"/> class over a <see cref="Stream"/>.
    /// </summary>
    /// <param name="stream">The source readable stream.</param>
    /// <param name="options">Optional JSON serializer options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public JsonLinesStreamReader(
        Stream stream,
        JsonSerializerOptions? options = null,
        CancellationToken cancellationToken = default)
        : this(stream, options, leaveOpen: true, cancellationToken)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="JsonLinesStreamReader{TRecord}"/> class over a <see cref="Stream"/> with explicit leaveOpen configuration.
    /// </summary>
    /// <param name="stream">The source readable stream.</param>
    /// <param name="options">Optional JSON serializer options.</param>
    /// <param name="leaveOpen">True to leave the source stream open after the reader completes.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public JsonLinesStreamReader(
        Stream stream,
        JsonSerializerOptions? options,
        bool leaveOpen,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream);

        _pipeReader = PipeReader.Create(stream, new StreamPipeReaderOptions(bufferSize: 65536, leaveOpen: leaveOpen));
        _ownsPipeReader = true;
        _options = options ?? DefaultSerializerOptions;
        _defaultCancellationToken = cancellationToken;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="JsonLinesStreamReader{TRecord}"/> class over an existing <see cref="PipeReader"/>.
    /// </summary>
    /// <param name="pipeReader">The source pipe reader.</param>
    /// <param name="options">Optional JSON serializer options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public JsonLinesStreamReader(
        PipeReader pipeReader,
        JsonSerializerOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        _pipeReader = pipeReader ?? throw new ArgumentNullException(nameof(pipeReader));
        _ownsPipeReader = false;
        _options = options ?? DefaultSerializerOptions;
        _defaultCancellationToken = cancellationToken;
    }

    /// <summary>
    /// Streams records asynchronously, throwing <see cref="JsonException"/> if malformed JSON is encountered.
    /// </summary>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>An async enumerable yielding valid <typeparamref name="TRecord"/> records.</returns>
    public async IAsyncEnumerable<TRecord> ReadAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(_defaultCancellationToken, cancellationToken);
        var ct = linkedCts.Token;

        await foreach (var item in ReadLineItemsAsync(ct).ConfigureAwait(false))
        {
            if (!item.IsSuccess)
            {
                throw new JsonException(item.Error?.ErrorMessage ?? $"JSON deserialization error on line {item.LineNumber}.");
            }

            if (item.Record != null)
            {
                yield return item.Record;
            }
        }
    }

    /// <summary>
    /// Streams each line result including captured parsing errors, line numbers, and raw snippets without throwing.
    /// </summary>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>An async enumerable yielding <see cref="JsonLineItem{TRecord}"/> instances.</returns>
    public async IAsyncEnumerable<JsonLineItem<TRecord>> ReadLineItemsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(_defaultCancellationToken, cancellationToken);
        var ct = linkedCts.Token;

        long lineNumber = 0;
        var readerOptions = new JsonReaderOptions
        {
            AllowTrailingCommas = _options.AllowTrailingCommas,
            CommentHandling = _options.ReadCommentHandling,
            MaxDepth = _options.MaxDepth != 0 ? _options.MaxDepth : 64
        };

        try
        {
            while (true)
            {
                ReadResult readResult = await _pipeReader.ReadAsync(ct).ConfigureAwait(false);
                ReadOnlySequence<byte> buffer = readResult.Buffer;

                while (TryReadLine(ref buffer, out ReadOnlySequence<byte> line))
                {
                    lineNumber++;

                    line = TrimTrailingCarriageReturn(line);
                    if (IsEmptyOrWhiteSpace(line))
                    {
                        continue;
                    }

                    yield return ParseLine(line, lineNumber, readerOptions);
                }

                _pipeReader.AdvanceTo(buffer.Start, buffer.End);

                if (readResult.IsCompleted)
                {
                    // Process any remaining bytes at EOF that lacked a trailing newline
                    if (!buffer.IsEmpty)
                    {
                        lineNumber++;
                        ReadOnlySequence<byte> remaining = TrimTrailingCarriageReturn(buffer);
                        if (!IsEmptyOrWhiteSpace(remaining))
                        {
                            yield return ParseLine(remaining, lineNumber, readerOptions);
                        }
                    }

                    break;
                }
            }
        }
        finally
        {
            if (_ownsPipeReader)
            {
                await _pipeReader.CompleteAsync().ConfigureAwait(false);
            }
        }
    }

    /// <inheritdoc/>
    public IAsyncEnumerator<TRecord> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        return ReadAsync(cancellationToken).GetAsyncEnumerator(cancellationToken);
    }

    /// <summary>
    /// Parses a single line sequence into <typeparamref name="TRecord"/> using Utf8JsonReader and JsonSerializer.
    /// </summary>
    private JsonLineItem<TRecord> ParseLine(
        ReadOnlySequence<byte> line,
        long lineNumber,
        JsonReaderOptions readerOptions)
    {
        try
        {
            var jsonReader = new Utf8JsonReader(line, readerOptions);
            var record = JsonSerializer.Deserialize<TRecord>(ref jsonReader, _options);

            if (record == null)
            {
                var nullError = new IngestRowError(lineNumber, null, "null", $"Record on line {lineNumber} deserialized to null.");
                return JsonLineItem<TRecord>.Failure(lineNumber, nullError);
            }

            // Ensure no unexpected extra tokens exist after the deserialized record on this line
            while (jsonReader.Read())
            {
                if (jsonReader.TokenType != JsonTokenType.Comment && jsonReader.TokenType != JsonTokenType.None)
                {
                    throw new JsonException($"Unexpected token '{jsonReader.TokenType}' found after record on line {lineNumber}.");
                }
            }

            return JsonLineItem<TRecord>.Success(lineNumber, record);
        }
        catch (Exception ex)
        {
            string rawSnippet = ExtractRawSnippet(line);
            string? propertyPath = (ex as JsonException)?.Path;

            var rowError = new IngestRowError(
                lineNumber,
                propertyPath,
                rawSnippet,
                $"JSON parsing failed on row {lineNumber}: {ex.Message}");

            return JsonLineItem<TRecord>.Failure(lineNumber, rowError);
        }
    }

    /// <summary>
    /// Slices the next line from the buffer up to the newline character.
    /// </summary>
    private static bool TryReadLine(ref ReadOnlySequence<byte> buffer, out ReadOnlySequence<byte> line)
    {
        SequencePosition? position = buffer.PositionOf((byte)'\n');
        if (position == null)
        {
            line = default;
            return false;
        }

        line = buffer.Slice(0, position.Value);
        buffer = buffer.Slice(buffer.GetPosition(1, position.Value));
        return true;
    }

    /// <summary>
    /// Trims trailing carriage return ('\r') if present.
    /// </summary>
    private static ReadOnlySequence<byte> TrimTrailingCarriageReturn(ReadOnlySequence<byte> line)
    {
        if (line.IsEmpty) return line;

        if (line.IsSingleSegment)
        {
            var span = line.FirstSpan;
            if (span.Length > 0 && span[^1] == (byte)'\r')
            {
                return line.Slice(0, span.Length - 1);
            }
            return line;
        }

        var lastPos = line.GetPosition(line.Length - 1);
        var lastSeq = line.Slice(lastPos);
        if (lastSeq.FirstSpan[0] == (byte)'\r')
        {
            return line.Slice(0, line.Length - 1);
        }

        return line;
    }

    /// <summary>
    /// Checks if a sequence is empty or consists purely of whitespace characters without allocating strings.
    /// </summary>
    private static bool IsEmptyOrWhiteSpace(ReadOnlySequence<byte> sequence)
    {
        if (sequence.IsEmpty) return true;

        if (sequence.IsSingleSegment)
        {
            var span = sequence.FirstSpan;
            for (int i = 0; i < span.Length; i++)
            {
                byte b = span[i];
                if (b != (byte)' ' && b != (byte)'\t' && b != (byte)'\r' && b != (byte)'\n')
                {
                    return false;
                }
            }
            return true;
        }

        foreach (var memory in sequence)
        {
            var span = memory.Span;
            for (int i = 0; i < span.Length; i++)
            {
                byte b = span[i];
                if (b != (byte)' ' && b != (byte)'\t' && b != (byte)'\r' && b != (byte)'\n')
                {
                    return false;
                }
            }
        }

        return true;
    }

    /// <summary>
    /// Extracts a safe string representation of the line for error manifests without allocating unbounded memory.
    /// </summary>
    private static string ExtractRawSnippet(ReadOnlySequence<byte> line)
    {
        const int maxSnippetLength = 2048;
        if (line.Length > maxSnippetLength)
        {
            return Encoding.UTF8.GetString(line.Slice(0, maxSnippetLength)) + "... [truncated]";
        }

        return Encoding.UTF8.GetString(line);
    }
}
