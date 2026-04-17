package se.kuseman.payloadbuilder.catalog.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.catalog.TestUtils;
import se.kuseman.payloadbuilder.catalog.kafka.KafkaOptions.ExecutionMode;
import se.kuseman.payloadbuilder.catalog.kafka.KafkaOptions.Format;
import se.kuseman.payloadbuilder.catalog.kafka.KafkaOptions.OnError;
import se.kuseman.payloadbuilder.catalog.kafka.KafkaOptions.SortOrder;
import se.kuseman.payloadbuilder.test.ExpressionTestUtils;

/** Test of {@link KafkaOptions} */
class KafkaOptionsTest
{
    private IExecutionContext createContext()
    {
        return TestUtils.mockExecutionContext("kafka", Map.of(), 0, null);
    }

    @Test
    void test_defaults()
    {
        IExecutionContext context = createContext();
        KafkaOptions options = KafkaOptions.from(context, Collections.emptyList());

        assertEquals("earliest", options.start());
        assertEquals("latest", options.end());
        assertEquals(ExecutionMode.BATCH, options.mode());
        assertEquals(Format.JSON, options.format());
        assertEquals(OnError.FAIL, options.onError());
        assertEquals(1000, options.pollTimeoutMs());
        assertEquals(SortOrder.OLDEST, options.sortOrder());
        assertEquals(IExecutionContext.DEFAULT_BATCH_SIZE, options.tailCount());
    }

    @Test
    void test_start_option()
    {
        IExecutionContext context = createContext();
        List<Option> opts = new ArrayList<>();
        opts.add(new Option(KafkaOptions.START, ExpressionTestUtils.createStringExpression("latest")));

        KafkaOptions options = KafkaOptions.from(context, opts);
        assertEquals("latest", options.start());
    }

    @Test
    void test_end_option()
    {
        IExecutionContext context = createContext();
        List<Option> opts = new ArrayList<>();
        opts.add(new Option(KafkaOptions.END, ExpressionTestUtils.createStringExpression("2024-01-01T00:00:00Z")));

        KafkaOptions options = KafkaOptions.from(context, opts);
        assertEquals("2024-01-01T00:00:00Z", options.end());
    }

    @Test
    void test_mode_batch()
    {
        IExecutionContext context = createContext();
        List<Option> opts = new ArrayList<>();
        opts.add(new Option(KafkaOptions.MODE, ExpressionTestUtils.createStringExpression("batch")));

        KafkaOptions options = KafkaOptions.from(context, opts);
        assertEquals(ExecutionMode.BATCH, options.mode());
    }

    @Test
    void test_mode_stream()
    {
        IExecutionContext context = createContext();
        List<Option> opts = new ArrayList<>();
        opts.add(new Option(KafkaOptions.MODE, ExpressionTestUtils.createStringExpression("stream")));

        KafkaOptions options = KafkaOptions.from(context, opts);
        assertEquals(ExecutionMode.STREAM, options.mode());
    }

    @Test
    void test_format_avro()
    {
        IExecutionContext context = createContext();
        List<Option> opts = new ArrayList<>();
        opts.add(new Option(KafkaOptions.FORMAT, ExpressionTestUtils.createStringExpression("avro")));

        KafkaOptions options = KafkaOptions.from(context, opts);
        assertEquals(Format.AVRO, options.format());
    }

    @Test
    void test_format_raw()
    {
        IExecutionContext context = createContext();
        List<Option> opts = new ArrayList<>();
        opts.add(new Option(KafkaOptions.FORMAT, ExpressionTestUtils.createStringExpression("raw")));

        KafkaOptions options = KafkaOptions.from(context, opts);
        assertEquals(Format.RAW, options.format());
    }

    @Test
    void test_on_error_skip()
    {
        IExecutionContext context = createContext();
        List<Option> opts = new ArrayList<>();
        opts.add(new Option(KafkaOptions.ON_ERROR, ExpressionTestUtils.createStringExpression("skip")));

        KafkaOptions options = KafkaOptions.from(context, opts);
        assertEquals(OnError.SKIP, options.onError());
    }

    @Test
    void test_on_error_null()
    {
        IExecutionContext context = createContext();
        List<Option> opts = new ArrayList<>();
        opts.add(new Option(KafkaOptions.ON_ERROR, ExpressionTestUtils.createStringExpression("null")));

        KafkaOptions options = KafkaOptions.from(context, opts);
        assertEquals(OnError.NULL, options.onError());
    }

    @Test
    void test_poll_timeout()
    {
        IExecutionContext context = createContext();
        List<Option> opts = new ArrayList<>();
        opts.add(new Option(KafkaOptions.POLL_TIMEOUT, ExpressionTestUtils.createIntegerExpression(5000)));

        KafkaOptions options = KafkaOptions.from(context, opts);
        assertEquals(5000, options.pollTimeoutMs());
    }

    @Test
    void test_sort_order_newest()
    {
        IExecutionContext context = createContext();
        List<Option> opts = new ArrayList<>();
        opts.add(new Option(KafkaOptions.SORT_ORDER, ExpressionTestUtils.createStringExpression("newest")));

        KafkaOptions options = KafkaOptions.from(context, opts);
        assertEquals(SortOrder.NEWEST, options.sortOrder());
    }

    @Test
    void test_tail_count()
    {
        IExecutionContext context = createContext();
        List<Option> opts = new ArrayList<>();
        opts.add(new Option(KafkaOptions.TAIL_COUNT, ExpressionTestUtils.createIntegerExpression(250)));

        KafkaOptions options = KafkaOptions.from(context, opts);
        assertEquals(250, options.tailCount());
    }

    @Test
    void test_invalid_mode()
    {
        IExecutionContext context = createContext();
        List<Option> opts = new ArrayList<>();
        opts.add(new Option(KafkaOptions.MODE, ExpressionTestUtils.createStringExpression("invalid")));

        assertThrows(IllegalArgumentException.class, () -> KafkaOptions.from(context, opts));
    }

    @Test
    void test_invalid_format()
    {
        IExecutionContext context = createContext();
        List<Option> opts = new ArrayList<>();
        opts.add(new Option(KafkaOptions.FORMAT, ExpressionTestUtils.createStringExpression("invalid")));

        assertThrows(IllegalArgumentException.class, () -> KafkaOptions.from(context, opts));
    }

    @Test
    void test_invalid_sort_order()
    {
        IExecutionContext context = createContext();
        List<Option> opts = new ArrayList<>();
        opts.add(new Option(KafkaOptions.SORT_ORDER, ExpressionTestUtils.createStringExpression("invalid")));

        assertThrows(IllegalArgumentException.class, () -> KafkaOptions.from(context, opts));
    }

    @Test
    void test_invalid_tail_count()
    {
        IExecutionContext context = createContext();
        List<Option> opts = new ArrayList<>();
        opts.add(new Option(KafkaOptions.TAIL_COUNT, ExpressionTestUtils.createIntegerExpression(0)));

        assertThrows(IllegalArgumentException.class, () -> KafkaOptions.from(context, opts));
    }

    @Test
    void test_newest_not_supported_for_stream_mode()
    {
        IExecutionContext context = createContext();
        List<Option> opts = new ArrayList<>();
        opts.add(new Option(KafkaOptions.MODE, ExpressionTestUtils.createStringExpression("stream")));
        opts.add(new Option(KafkaOptions.SORT_ORDER, ExpressionTestUtils.createStringExpression("newest")));

        assertThrows(IllegalArgumentException.class, () -> KafkaOptions.from(context, opts));
    }

    @Test
    void test_multiple_options()
    {
        IExecutionContext context = createContext();
        List<Option> opts = new ArrayList<>();
        opts.add(new Option(KafkaOptions.START, ExpressionTestUtils.createStringExpression("group:analytics")));
        opts.add(new Option(KafkaOptions.END, ExpressionTestUtils.createStringExpression("1000")));
        opts.add(new Option(KafkaOptions.MODE, ExpressionTestUtils.createStringExpression("stream")));
        opts.add(new Option(KafkaOptions.FORMAT, ExpressionTestUtils.createStringExpression("avro")));
        opts.add(new Option(KafkaOptions.ON_ERROR, ExpressionTestUtils.createStringExpression("skip")));
        opts.add(new Option(KafkaOptions.POLL_TIMEOUT, ExpressionTestUtils.createIntegerExpression(3000)));
        opts.add(new Option(KafkaOptions.SORT_ORDER, ExpressionTestUtils.createStringExpression("oldest")));
        opts.add(new Option(KafkaOptions.TAIL_COUNT, ExpressionTestUtils.createIntegerExpression(1234)));

        KafkaOptions options = KafkaOptions.from(context, opts);
        assertEquals("group:analytics", options.start());
        assertEquals("1000", options.end());
        assertEquals(ExecutionMode.STREAM, options.mode());
        assertEquals(Format.AVRO, options.format());
        assertEquals(OnError.SKIP, options.onError());
        assertEquals(3000, options.pollTimeoutMs());
        assertEquals(SortOrder.OLDEST, options.sortOrder());
        assertEquals(1234, options.tailCount());
    }
}
