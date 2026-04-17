package se.kuseman.payloadbuilder.catalog.kafka;

import java.util.List;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Parses and holds WITH clause options for Kafka queries */
record KafkaOptions(String start, String end, ExecutionMode mode, Format format, OnError onError, int pollTimeoutMs, int batchSize, SortOrder sortOrder, int tailCount)
{

    static final QualifiedName START = QualifiedName.of("start");
    static final QualifiedName END = QualifiedName.of("end");
    static final QualifiedName MODE = QualifiedName.of("mode");
    static final QualifiedName FORMAT = QualifiedName.of("format");
    static final QualifiedName ON_ERROR = QualifiedName.of("on_error");
    static final QualifiedName POLL_TIMEOUT = QualifiedName.of("poll_timeout");
    static final QualifiedName SORT_ORDER = QualifiedName.of("sort_order");
    static final QualifiedName TAIL_COUNT = QualifiedName.of("tail_count");

    private static final String DEFAULT_START = "earliest";
    private static final String DEFAULT_END = "latest";
    private static final int DEFAULT_POLL_TIMEOUT_MS = 1000;

    /** Parse options from WITH clause */
    static KafkaOptions from(IExecutionContext context, List<Option> options)
    {
        String start = getStringOption(context, options, START, DEFAULT_START);
        String end = getStringOption(context, options, END, DEFAULT_END);
        ExecutionMode mode = parseMode(getStringOption(context, options, MODE, "batch"));
        Format format = parseFormat(getStringOption(context, options, FORMAT, "json"));
        OnError onError = parseOnError(getStringOption(context, options, ON_ERROR, "fail"));
        int pollTimeoutMs = getIntOption(context, options, POLL_TIMEOUT, DEFAULT_POLL_TIMEOUT_MS);
        int batchSize = getIntOption(context, options, IExecutionContext.BATCH_SIZE, IExecutionContext.DEFAULT_BATCH_SIZE);
        SortOrder sortOrder = parseSortOrder(getStringOption(context, options, SORT_ORDER, "oldest"));
        int tailCount = getIntOption(context, options, TAIL_COUNT, batchSize);

        if (tailCount <= 0)
        {
            throw new IllegalArgumentException("Invalid tail_count: " + tailCount + ". Value must be > 0");
        }

        if (mode == ExecutionMode.STREAM
                && sortOrder == SortOrder.NEWEST)
        {
            throw new IllegalArgumentException("sort_order='newest' is not supported in stream mode");
        }

        return new KafkaOptions(start, end, mode, format, onError, pollTimeoutMs, batchSize, sortOrder, tailCount);
    }

    KafkaOptions withSortOrder(SortOrder sortOrder)
    {
        if (sortOrder == null)
        {
            return this;
        }

        if (mode == ExecutionMode.STREAM
                && sortOrder == SortOrder.NEWEST)
        {
            throw new IllegalArgumentException("sort_order='newest' is not supported in stream mode");
        }

        return new KafkaOptions(start, end, mode, format, onError, pollTimeoutMs, batchSize, sortOrder, tailCount);
    }

    private static ValueVector evalOption(IExecutionContext context, List<Option> options, QualifiedName name)
    {
        for (Option option : options)
        {
            if (option.getOption()
                    .equalsIgnoreCase(name))
            {
                return option.getValueExpression()
                        .eval(context);
            }
        }
        return null;
    }

    private static String getStringOption(IExecutionContext context, List<Option> options, QualifiedName name, String defaultValue)
    {
        ValueVector v = evalOption(context, options, name);
        if (v != null
                && !v.isNull(0))
        {
            return v.valueAsString(0);
        }
        return defaultValue;
    }

    private static int getIntOption(IExecutionContext context, List<Option> options, QualifiedName name, int defaultValue)
    {
        ValueVector v = evalOption(context, options, name);
        if (v != null
                && !v.isNull(0))
        {
            return v.getInt(0);
        }
        return defaultValue;
    }

    private static ExecutionMode parseMode(String value)
    {
        try
        {
            return ExecutionMode.valueOf(value.toUpperCase());
        }
        catch (IllegalArgumentException e)
        {
            throw new IllegalArgumentException("Invalid mode: '" + value + "'. Supported: batch, stream");
        }
    }

    private static Format parseFormat(String value)
    {
        try
        {
            return Format.valueOf(value.toUpperCase());
        }
        catch (IllegalArgumentException e)
        {
            throw new IllegalArgumentException("Invalid format: '" + value + "'. Supported: json, avro, raw");
        }
    }

    private static OnError parseOnError(String value)
    {
        try
        {
            return OnError.valueOf(value.toUpperCase());
        }
        catch (IllegalArgumentException e)
        {
            throw new IllegalArgumentException("Invalid on_error: '" + value + "'. Supported: fail, skip, null");
        }
    }

    private static SortOrder parseSortOrder(String value)
    {
        try
        {
            return SortOrder.valueOf(value.toUpperCase());
        }
        catch (IllegalArgumentException e)
        {
            throw new IllegalArgumentException("Invalid sort_order: '" + value + "'. Supported: oldest, newest");
        }
    }

    /** Execution mode */
    enum ExecutionMode
    {
        BATCH,
        STREAM
    }

    /** Value deserialization format */
    enum Format
    {
        JSON,
        AVRO,
        RAW
    }

    /** Error handling strategy for deserialization errors */
    enum OnError
    {
        FAIL,
        SKIP,
        NULL
    }

    /** Read order strategy */
    enum SortOrder
    {
        OLDEST,
        NEWEST
    }
}
