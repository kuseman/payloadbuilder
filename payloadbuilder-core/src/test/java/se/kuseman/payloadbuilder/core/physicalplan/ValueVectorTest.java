package se.kuseman.payloadbuilder.core.physicalplan;

import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.io.StringWriter;
import java.time.Instant;
import java.time.ZoneId;
import java.util.TimeZone;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.UTF8String;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.utils.VectorUtils;
import se.kuseman.payloadbuilder.core.JsonOutputWriter;

/** Test of {@link ValueVector} */
public class ValueVectorTest extends APhysicalPlanTest
{
    @Test
    public void test_string_implicit_casts()
    {
        ValueVector vv;

        vv = ValueVector.literalObject(ResolvedType.of(Type.Int), 1, 1);
        assertEquals("1", vv.getString(0)
                .toString());
        vv = ValueVector.literalObject(ResolvedType.of(Type.Long), 1, 1);
        assertEquals("1", vv.getString(0)
                .toString());
        vv = ValueVector.literalObject(ResolvedType.of(Type.Float), 1, 1);
        assertEquals("1.0", vv.getString(0)
                .toString());
        vv = ValueVector.literalObject(ResolvedType.of(Type.Double), 1, 1);
        assertEquals("1.0", vv.getString(0)
                .toString());
        vv = ValueVector.literalObject(ResolvedType.of(Type.Boolean), true, 1);
        assertEquals("true", vv.getString(0)
                .toString());
    }

    @Test
    public void test_datetime_implicit_casts()
    {
        TimeZone defaultTimezone = TimeZone.getDefault();
        try
        {
            TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of("Europe/Berlin")));

            ValueVector vv;

            vv = ValueVector.literalObject(ResolvedType.of(Type.Long), 1_680_193_853_072L, 1);
            assertEquals("2023-03-30T16:30:53.072Z", vv.getDateTime(0)
                    .toString());
            vv = ValueVector.literalObject(ResolvedType.of(Type.Any), Instant.ofEpochMilli(1_680_193_853_072L)
                    .atZone(ZoneId.of("UTC")), 1);
            assertEquals("2023-03-30T16:30:53.072Z", vv.getDateTime(0)
                    .toString());
            vv = ValueVector.literalObject(ResolvedType.of(Type.String), "2023-03-30T16:30:53.072+02:00", 1);
            assertEquals("2023-03-30T16:30:53.072", vv.getDateTime(0)
                    .toString());
        }
        finally
        {
            TimeZone.setDefault(defaultTimezone);
        }
    }

    @Test
    public void test_get_boolean_implicit_casts()
    {
        ValueVector vv;

        // Strings
        vv = ValueVector.literalObject(ResolvedType.of(Type.String), "y", 1);
        assertTrue(vv.getBoolean(0));
        vv = ValueVector.literalObject(ResolvedType.of(Type.String), "N", 1);
        assertFalse(vv.getBoolean(0));
        vv = ValueVector.literalObject(ResolvedType.of(Type.String), "yeS", 1);
        assertTrue(vv.getBoolean(0));
        vv = ValueVector.literalObject(ResolvedType.of(Type.String), "No", 1);
        assertFalse(vv.getBoolean(0));
        vv = ValueVector.literalObject(ResolvedType.of(Type.String), "TRUe", 1);
        assertTrue(vv.getBoolean(0));
        vv = ValueVector.literalObject(ResolvedType.of(Type.String), "FAlse", 1);
        assertFalse(vv.getBoolean(0));
        vv = ValueVector.literalObject(ResolvedType.of(Type.String), "1", 1);
        assertTrue(vv.getBoolean(0));
        vv = ValueVector.literalObject(ResolvedType.of(Type.String), "0", 1);
        assertFalse(vv.getBoolean(0));
        vv = ValueVector.literalObject(ResolvedType.of(Type.String), "nono", 1);
        try
        {
            assertFalse(vv.getBoolean(0));
            fail("Should fail with cast exception");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot cast 'nono' to Boolean"));
        }

        // Int
        vv = ValueVector.literalObject(ResolvedType.of(Type.Int), 10, 1);
        assertTrue(vv.getBoolean(0));
        vv = ValueVector.literalObject(ResolvedType.of(Type.Int), 0, 1);
        assertFalse(vv.getBoolean(0));
        // Long
        vv = ValueVector.literalObject(ResolvedType.of(Type.Long), 10, 1);
        assertTrue(vv.getBoolean(0));
        vv = ValueVector.literalObject(ResolvedType.of(Type.Long), 0, 1);
        assertFalse(vv.getBoolean(0));

        // Non supported
        vv = ValueVector.literalObject(ResolvedType.of(Type.Float), 2F, 1);
        try
        {
            assertFalse(vv.getBoolean(0));
            fail("Should fail with cast exception");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot cast Float to Boolean"));
        }
    }

    @Test
    public void test_write()
    {
        ValueVector vector = ValueVector.literalInt(10, 5);

        StringWriter writer = new StringWriter();
        JsonOutputWriter w = new JsonOutputWriter(writer);
        vector.write(w, context);
        w.close();

        assertEquals("[10,10,10,10,10]", writer.toString());

        vector = ValueVector.literalNull(ResolvedType.of(Type.Int), 5);

        writer = new StringWriter();
        w = new JsonOutputWriter(writer);
        vector.write(w, context);
        w.close();

        assertEquals("[null,null,null,null,null]", writer.toString());
    }

    @Test
    public void test_concat()
    {
        ValueVector vector1 = ValueVector.literalInt(10, 5);
        ValueVector vector2 = ValueVector.literalNull(ResolvedType.of(Type.Int), 5);
        ValueVector vector = VectorUtils.concat(vector1, vector2);

        assertEquals(10, vector.size());

        assertVectorsEquals(vv(ResolvedType.of(Type.Int), 10, 10, 10, 10, 10, null, null, null, null, null), vector);
    }

    @Test
    public void test_concat_different_sizes()
    {
        ValueVector vector1 = ValueVector.literalInt(10, 3);
        ValueVector vector2 = ValueVector.literalNull(ResolvedType.of(Type.Int), 5);
        ValueVector vector = VectorUtils.concat(vector1, vector2);

        assertEquals(8, vector.size());

        assertVectorsEquals(vv(ResolvedType.of(Type.Int), 10, 10, 10, null, null, null, null, null), vector);
    }

    @Test
    public void test_concat_verify_objects_method_gets_proxied()
    {
        MutableInt calls1 = new MutableInt();
        MutableInt calls2 = new MutableInt();
        ValueVector vv1 = new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                return ResolvedType.of(Type.Any);
            }

            @Override
            public int size()
            {
                return 2;
            }

            @Override
            public boolean isNull(int row)
            {
                return false;
            }

            @Override
            public Object getValue(int row)
            {
                return row;
            }

            @Override
            public UTF8String getString(int row)
            {
                calls1.increment();
                return ValueVector.super.getString(row);
            }
        };

        ValueVector vv2 = new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                return ResolvedType.of(Type.Any);
            }

            @Override
            public int size()
            {
                return 2;
            }

            @Override
            public boolean isNull(int row)
            {
                return false;
            }

            @Override
            public Object getValue(int row)
            {
                return row;
            }

            @Override
            public UTF8String getString(int row)
            {
                calls2.increment();
                return ValueVector.super.getString(row);
            }
        };
        ValueVector vector = VectorUtils.concat(vv1, vv2);

        assertEquals(4, vector.size());
        for (int i = 0; i < 4; i++)
        {
            UTF8String ref = vector.getString(i);
            assertNotNull(ref);
        }

        assertEquals(2, calls1.intValue());
        assertEquals(2, calls2.intValue());
    }

    @Test(
            expected = IllegalArgumentException.class)
    public void test_concat_different_types()
    {
        ValueVector vector1 = ValueVector.literalInt(10, 5);
        ValueVector vector2 = ValueVector.literalNull(ResolvedType.of(Type.Boolean), 5);
        VectorUtils.concat(vector1, vector2);
    }

    @Test(
            expected = IllegalArgumentException.class)
    public void test_cardinality_non_boolean()
    {
        ValueVector.literalInt(5, 10)
                .getCardinality();
    }

    @Test
    public void test_cardinality()
    {
        assertEquals(ValueVector.literalBoolean(true, 10)
                .getCardinality(), 10);
    }

    @Test
    public void test_literals()
    {
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), true, true), ValueVector.literalBoolean(true, 2));
        assertVectorsEquals(vv(ResolvedType.of(Type.Int), 10, 10), ValueVector.literalInt(10, 2));
        assertVectorsEquals(vv(ResolvedType.of(Type.Long), 100L, 100L), ValueVector.literalLong(100L, 2));
        assertVectorsEquals(vv(ResolvedType.of(Type.Float), 1000F, 1000F), ValueVector.literalFloat(1000F, 2));
        assertVectorsEquals(vv(ResolvedType.of(Type.Double), 10_000D, 10_000D), ValueVector.literalDouble(10_000D, 2));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), "hello", "hello"), ValueVector.literalObject(ResolvedType.of(Type.Any), "hello", 2));
        assertVectorsEquals(vv(ResolvedType.of(Type.Int), null, null), ValueVector.literalNull(ResolvedType.of(Type.Int), 2));
    }
}
