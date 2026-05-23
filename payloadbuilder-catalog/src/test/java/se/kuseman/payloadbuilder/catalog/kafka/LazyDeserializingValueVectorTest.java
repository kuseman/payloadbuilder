package se.kuseman.payloadbuilder.catalog.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;

/** Test of {@link LazyDeserializingValueVector} */
class LazyDeserializingValueVectorTest
{
    @Test
    void test_type_and_size()
    {
        byte[][] payloads = { new byte[] { 1 }, new byte[] { 2 }, null };
        IRecordDeserializer deserializer = mock(IRecordDeserializer.class);

        LazyDeserializingValueVector vector = new LazyDeserializingValueVector(payloads, deserializer);

        assertEquals(ResolvedType.ANY, vector.type());
        assertEquals(3, vector.size());
    }

    @Test
    void test_null_handling()
    {
        byte[][] payloads = { null, new byte[] { 1 } };
        IRecordDeserializer deserializer = mock(IRecordDeserializer.class);

        LazyDeserializingValueVector vector = new LazyDeserializingValueVector(payloads, deserializer);

        assertTrue(vector.isNull(0));
        assertNull(vector.getAny(0));

        // null row should not trigger deserialization
        verify(deserializer, never()).deserializeValue(any());
    }

    @Test
    void test_lazy_deserialization()
    {
        byte[] payload = "{\"id\":1}".getBytes();
        byte[][] payloads = { payload };
        Map<String, Object> deserialized = Map.of("id", 1);

        IRecordDeserializer deserializer = mock(IRecordDeserializer.class);
        when(deserializer.deserializeValue(payload)).thenReturn(deserialized);

        LazyDeserializingValueVector vector = new LazyDeserializingValueVector(payloads, deserializer);

        // Before access: no deserialization
        verify(deserializer, never()).deserializeValue(any());

        // First access: triggers deserialization
        Object result = vector.getAny(0);
        assertEquals(deserialized, result);
        verify(deserializer, times(1)).deserializeValue(payload);

        // Second access: cached, no additional deserialization
        Object result2 = vector.getAny(0);
        assertEquals(deserialized, result2);
        verify(deserializer, times(1)).deserializeValue(payload);
    }

    @Test
    void test_only_accessed_rows_are_deserialized()
    {
        byte[] payload0 = "row0".getBytes();
        byte[] payload1 = "row1".getBytes();
        byte[] payload2 = "row2".getBytes();
        byte[][] payloads = { payload0, payload1, payload2 };

        IRecordDeserializer deserializer = mock(IRecordDeserializer.class);
        when(deserializer.deserializeValue(payload0)).thenReturn("value0");
        when(deserializer.deserializeValue(payload1)).thenReturn("value1");
        when(deserializer.deserializeValue(payload2)).thenReturn("value2");

        LazyDeserializingValueVector vector = new LazyDeserializingValueVector(payloads, deserializer);

        // Only access row 1
        assertEquals("value1", vector.getAny(1));

        // Row 0 and 2 should not be deserialized
        verify(deserializer, never()).deserializeValue(payload0);
        verify(deserializer, times(1)).deserializeValue(payload1);
        verify(deserializer, never()).deserializeValue(payload2);
    }
}
