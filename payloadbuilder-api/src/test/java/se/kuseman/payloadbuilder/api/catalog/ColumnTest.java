package se.kuseman.payloadbuilder.api.catalog;

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.catalog.Column.MetaData;

/** Test of {@link Column}. */
class ColumnTest
{
    @Test
    void test_metadata()
    {
        Column.MetaData metaData = new MetaData(emptyMap());
        assertTrue(metaData.isNullable());
        assertEquals(-1, metaData.getPrecision());
        assertEquals(-1, metaData.getScale());

        metaData = new MetaData(Map.of(MetaData.NULLABLE, false, MetaData.SCALE, 1, MetaData.PRECISION, 2, "key", "value"));
        assertFalse(metaData.isNullable());
        assertEquals(2, metaData.getPrecision());
        assertEquals(1, metaData.getScale());
        assertEquals("value", metaData.getMetaData("key"));
    }
}
