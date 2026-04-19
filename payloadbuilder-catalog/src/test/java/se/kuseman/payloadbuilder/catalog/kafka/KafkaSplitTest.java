package se.kuseman.payloadbuilder.catalog.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Test of {@link KafkaSplit} */
class KafkaSplitTest
{
    @Test
    void test_bounded_split()
    {
        KafkaSplit split = new KafkaSplit("orders", 0, 100, 200);

        assertEquals("orders", split.topic());
        assertEquals(0, split.partition());
        assertEquals(100, split.startOffset());
        assertEquals(200, split.endOffset());
        assertTrue(split.isBounded());
        assertEquals(100, split.estimatedRecordCount());
    }

    @Test
    void test_unbounded_split()
    {
        KafkaSplit split = new KafkaSplit("orders", 1, 0, Long.MAX_VALUE);

        assertFalse(split.isBounded());
        assertEquals(-1, split.estimatedRecordCount());
    }

    @Test
    void test_empty_split()
    {
        KafkaSplit split = new KafkaSplit("orders", 0, 500, 500);

        assertTrue(split.isBounded());
        assertEquals(0, split.estimatedRecordCount());
    }

    @Test
    void test_is_complete()
    {
        KafkaSplit split = new KafkaSplit("orders", 0, 100, 200);

        assertFalse(split.isComplete(99));
        assertFalse(split.isComplete(100));
        assertFalse(split.isComplete(199));
        assertTrue(split.isComplete(200));
        assertTrue(split.isComplete(201));
    }

    @Test
    void test_unbounded_is_never_complete()
    {
        KafkaSplit split = new KafkaSplit("orders", 0, 0, Long.MAX_VALUE);

        assertFalse(split.isComplete(0));
        assertFalse(split.isComplete(Long.MAX_VALUE - 1));
    }
}
