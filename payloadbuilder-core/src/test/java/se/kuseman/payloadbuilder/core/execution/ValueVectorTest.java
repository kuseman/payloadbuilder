package se.kuseman.payloadbuilder.core.execution;

import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.util.TimeZone;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.api.execution.EpochDateTime;
import se.kuseman.payloadbuilder.api.execution.EpochDateTimeOffset;
import se.kuseman.payloadbuilder.api.execution.ObjectVector;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Test of {@link ValueVector} */
public class ValueVectorTest extends APhysicalPlanTest
{
    @Test
    public void test_table_implicit_casts()
    {
        ValueVector vv;

        vv = ValueVector.literalAny(TupleVector.EMPTY);

        VectorTestUtils.assertTupleVectorsEquals(TupleVector.EMPTY, vv.getTable(0));

        try
        {
            vv = ValueVector.literalAny(10);
            vv.getTable(0);
            fail("Should fail cause of cast");

        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot cast 10 to Table"));
        }

        try
        {
            vv = ValueVector.literalArray(vv(Type.Int, 1), 1);
            vv.getTable(0);
            fail("Should fail cause of cast");

        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot cast Array to Table"));
        }

        try
        {
            vv = new ValueVector()
            {

                @Override
                public ResolvedType type()
                {
                    return ResolvedType.table(Schema.EMPTY);
                }

                @Override
                public int size()
                {
                    return 0;
                }

                @Override
                public boolean isNull(int row)
                {
                    return false;
                }
            };
            vv.getTable(0);
            fail("Should fail cause of cast");

        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("getTable not implemented on class se.kuseman.payloadbuilder.core.execution"));
        }

        vv = ValueVector.literalArray(vv(Type.Int), 0);
        VectorTestUtils.assertTupleVectorsEquals(TupleVector.EMPTY, vv.getTable(0));

        vv = ValueVector.literalObject(ObjectVector.wrap(TupleVector.CONSTANT), 1);
        VectorTestUtils.assertTupleVectorsEquals(TupleVector.EMPTY, vv.getTable(0));
    }

    @Test
    public void test_array_implicit_casts()
    {
        ValueVector vv;

        vv = ValueVector.literalAny(vv(Type.Int));

        VectorTestUtils.assertVectorsEquals(vv(Type.Int), vv.getArray(0));

        try
        {
            vv = ValueVector.literalAny(10);
            vv.getArray(0);
            fail("Should fail cause of cast");

        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot cast 10 to Array"));
        }

        try
        {
            vv = ValueVector.literalTable(TupleVector.EMPTY);
            vv.getArray(0);
            fail("Should fail cause of cast");

        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot cast Table to Array"));
        }

        try
        {
            vv = new ValueVector()
            {

                @Override
                public ResolvedType type()
                {
                    return ResolvedType.array(Type.Int);
                }

                @Override
                public int size()
                {
                    return 0;
                }

                @Override
                public boolean isNull(int row)
                {
                    return false;
                }
            };
            vv.getArray(0);
            fail("Should fail cause of cast");

        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("getArray not implemented on class se.kuseman.payloadbuilder.core.execution"));
        }
    }

    @Test
    public void test_object_implicit_casts()
    {
        ValueVector vv;

        vv = ValueVector.literalAny(ObjectVector.wrap(TupleVector.CONSTANT), 1);

        VectorTestUtils.assertObjectVectorsEquals(ObjectVector.wrap(TupleVector.CONSTANT), vv.getObject(0));

        try
        {
            vv = ValueVector.literalAny(10);
            vv.getObject(0);
            fail("Should fail cause of cast");

        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot cast 10 to Object"));
        }

        try
        {
            vv = ValueVector.literalTable(TupleVector.EMPTY);
            vv.getObject(0);
            fail("Should fail cause of cast");

        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot cast Table to Object"));
        }

        try
        {
            vv = new ValueVector()
            {

                @Override
                public ResolvedType type()
                {
                    return ResolvedType.object(Schema.EMPTY);
                }

                @Override
                public int size()
                {
                    return 0;
                }

                @Override
                public boolean isNull(int row)
                {
                    return false;
                }
            };
            vv.getObject(0);
            fail("Should fail cause of cast");

        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("getObject not implemented on class se.kuseman.payloadbuilder.core.execution"));
        }
    }

    @Test
    public void test_string_implicit_casts()
    {
        ValueVector vv;

        vv = ValueVector.literalInt(1, 1);
        assertEquals("1", vv.getString(0)
                .toString());
        vv = ValueVector.literalLong(1L, 1);
        assertEquals("1", vv.getString(0)
                .toString());
        vv = ValueVector.literalFloat(1.0F, 1);
        assertEquals("1.0", vv.getString(0)
                .toString());
        vv = ValueVector.literalDouble(1.0D, 1);
        assertEquals("1.0", vv.getString(0)
                .toString());
        vv = ValueVector.literalBoolean(true, 1);
        assertEquals("true", vv.getString(0)
                .toString());
    }

    @Test
    public void test_int_implicit_casts()
    {
        ValueVector vv;

        vv = ValueVector.literalInt(1, 1);
        assertEquals(1, vv.getInt(0));
        vv = ValueVector.literalLong(1L, 1);
        assertEquals(1, vv.getInt(0));
        vv = ValueVector.literalFloat(1.0F, 1);
        assertEquals(1, vv.getInt(0));
        vv = ValueVector.literalDouble(1.0D, 1);
        assertEquals(1, vv.getInt(0));
        vv = ValueVector.literalBoolean(true, 1);
        assertEquals(1, vv.getInt(0));
        vv = ValueVector.literalBoolean(false, 1);
        assertEquals(0, vv.getInt(0));
        vv = ValueVector.literalString("1", 1);
        assertEquals(1, vv.getInt(0));
        vv = ValueVector.literalAny(Decimal.from("1.100"), 1);
        assertEquals(1, vv.getInt(0));

        vv = ValueVector.literalDecimal(Decimal.from("1.100"), 1);
        assertEquals(1, vv.getInt(0));

        vv = ValueVector.literalAny(1, 100);
        assertEquals(100, vv.getInt(0));

        vv = ValueVector.literalAny(1, true);
        assertEquals(1, vv.getInt(0));
        vv = ValueVector.literalAny(1, false);
        assertEquals(0, vv.getInt(0));

        vv = ValueVector.literalAny(1, "100");
        assertEquals(100, vv.getInt(0));

        try
        {
            vv = ValueVector.literalString("nono", 1);
            vv.getInt(0);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot cast 'nono' to Int"));
        }

        try
        {
            vv = ValueVector.literalObject(ObjectVector.EMPTY, 1);
            vv.getInt(0);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot cast [se.kuseman.payloadbuilder.api.execution.ObjectVector"));
        }
    }

    @Test
    public void test_long_implicit_casts()
    {
        ValueVector vv;

        vv = ValueVector.literalInt(1, 1);
        assertEquals(1, vv.getLong(0));
        vv = ValueVector.literalLong(1L, 1);
        assertEquals(1, vv.getLong(0));
        vv = ValueVector.literalFloat(1.0F, 1);
        assertEquals(1, vv.getLong(0));
        vv = ValueVector.literalDouble(1.0D, 1);
        assertEquals(1, vv.getLong(0));
        vv = ValueVector.literalBoolean(true, 1);
        assertEquals(1, vv.getLong(0));
        vv = ValueVector.literalBoolean(false, 1);
        assertEquals(0, vv.getLong(0));
        vv = ValueVector.literalString("1", 1);
        assertEquals(1, vv.getLong(0));
        vv = ValueVector.literalAny(Decimal.from("1.100"), 1);
        assertEquals(1, vv.getLong(0));

        vv = ValueVector.literalDecimal(Decimal.from("1.100"), 1);
        assertEquals(1, vv.getLong(0));

        vv = ValueVector.literalAny(1, 100);
        assertEquals(100, vv.getLong(0));

        vv = ValueVector.literalAny(1, "100");
        assertEquals(100, vv.getLong(0));

        vv = ValueVector.literalAny(1, true);
        assertEquals(1L, vv.getLong(0));
        vv = ValueVector.literalAny(1, false);
        assertEquals(0L, vv.getLong(0));

        try
        {
            vv = ValueVector.literalString("nono", 1);
            vv.getLong(0);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot cast 'nono' to Long"));
        }

        try
        {
            vv = ValueVector.literalObject(ObjectVector.EMPTY, 1);
            vv.getLong(0);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot cast type Object to Long"));
        }
    }

    @Test
    public void test_decimal_implicit_casts()
    {
        ValueVector vv;

        vv = ValueVector.literalInt(1, 1);
        assertEquals(Decimal.from("1.000000"), vv.getDecimal(0));
        vv = ValueVector.literalLong(1L, 1);
        assertEquals(Decimal.from("1.000000"), vv.getDecimal(0));
        vv = ValueVector.literalFloat(1.0F, 1);
        assertEquals(Decimal.from("1.000000"), vv.getDecimal(0));
        vv = ValueVector.literalDouble(1.0D, 1);
        assertEquals(Decimal.from("1.000000"), vv.getDecimal(0));
        vv = ValueVector.literalBoolean(true, 1);
        assertEquals(Decimal.from("1.000000"), vv.getDecimal(0));
        vv = ValueVector.literalBoolean(false, 1);
        assertEquals(Decimal.from("0.000000"), vv.getDecimal(0));
        vv = ValueVector.literalString("1.100", 1);
        assertEquals(Decimal.from("1.100"), vv.getDecimal(0));

        vv = ValueVector.literalAny(new BigDecimal("1.100"), 1);
        assertEquals(Decimal.from("1.100"), vv.getDecimal(0));

        vv = ValueVector.literalAny(1.100D, 1);
        assertEquals(Decimal.from("1.100000"), vv.getDecimal(0));
        try
        {
            vv = ValueVector.literalAny(Instant.ofEpochMilli(1_600_000_000L), 1);
            vv.getDecimal(0);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot cast '1970-01-19T12:26:40Z' to Decimal"));
        }

        try
        {
            vv = ValueVector.literalString("nono", 1);
            vv.getDecimal(0);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot cast 'nono' to Decimal"));
        }
    }

    @Test
    public void test_float_implicit_casts()
    {
        ValueVector vv;

        vv = ValueVector.literalInt(1, 1);
        assertEquals(1F, vv.getFloat(0), 0);
        vv = ValueVector.literalLong(1L, 1);
        assertEquals(1F, vv.getFloat(0), 0);
        vv = ValueVector.literalFloat(1.0F, 1);
        assertEquals(1F, vv.getFloat(0), 0);
        vv = ValueVector.literalDouble(1.0D, 1);
        assertEquals(1F, vv.getFloat(0), 0);
        vv = ValueVector.literalBoolean(true, 1);
        assertEquals(1F, vv.getFloat(0), 0);
        vv = ValueVector.literalBoolean(false, 1);
        assertEquals(0F, vv.getFloat(0), 0);
        vv = ValueVector.literalString("1", 1);
        assertEquals(1F, vv.getFloat(0), 0);
        vv = ValueVector.literalAny(Decimal.from("1.100"), 1);
        assertEquals(1.1F, vv.getFloat(0), 0);

        vv = ValueVector.literalDecimal(Decimal.from("1.100"), 1);
        assertEquals(1.1F, vv.getFloat(0), 0);

        vv = ValueVector.literalAny(1, 100);
        assertEquals(100, vv.getFloat(0), 0.01);

        vv = ValueVector.literalAny(1, "100");
        assertEquals(100, vv.getFloat(0), 0.01);

        vv = ValueVector.literalAny(1, true);
        assertEquals(1.0F, vv.getFloat(0), 0.0);
        vv = ValueVector.literalAny(1, false);
        assertEquals(0.0F, vv.getFloat(0), 0.0);

        try
        {
            vv = ValueVector.literalString("nono", 1);
            vv.getFloat(0);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot cast 'nono' to Float"));
        }

        try
        {
            vv = ValueVector.literalObject(ObjectVector.EMPTY, 1);
            vv.getFloat(0);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot cast type Object to Float"));
        }
    }

    @Test
    public void test_double_implicit_casts()
    {
        ValueVector vv;

        vv = ValueVector.literalInt(1, 1);
        assertEquals(1D, vv.getDouble(0), 0);
        vv = ValueVector.literalLong(1L, 1);
        assertEquals(1D, vv.getDouble(0), 0);
        vv = ValueVector.literalFloat(1.0F, 1);
        assertEquals(1D, vv.getDouble(0), 0);
        vv = ValueVector.literalDouble(1.0D, 1);
        assertEquals(1D, vv.getDouble(0), 0);
        vv = ValueVector.literalBoolean(true, 1);
        assertEquals(1D, vv.getDouble(0), 0);
        vv = ValueVector.literalBoolean(false, 1);
        assertEquals(0D, vv.getDouble(0), 0);
        vv = ValueVector.literalString("1", 1);
        assertEquals(1D, vv.getDouble(0), 0);
        vv = ValueVector.literalAny(Decimal.from("1.100"), 1);
        assertEquals(1.1D, vv.getDouble(0), 0);

        vv = ValueVector.literalDecimal(Decimal.from("1.100"), 1);
        assertEquals(1.1D, vv.getDouble(0), 0);

        vv = ValueVector.literalAny(1, 100);
        assertEquals(100, vv.getDouble(0), 0);

        vv = ValueVector.literalAny(1, "100");
        assertEquals(100, vv.getDouble(0), 0);

        vv = ValueVector.literalAny(1, true);
        assertEquals(1.0D, vv.getDouble(0), 0.0);
        vv = ValueVector.literalAny(1, false);
        assertEquals(0.0D, vv.getDouble(0), 0.0);

        try
        {
            vv = ValueVector.literalString("nono", 1);
            vv.getDouble(0);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot cast 'nono' to Double"));
        }

        try
        {
            vv = ValueVector.literalObject(ObjectVector.EMPTY, 1);
            vv.getDouble(0);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot cast type Object to Double"));
        }
    }

    @Test
    public void test_datetime_implicit_casts()
    {
        TimeZone defaultTimezone = TimeZone.getDefault();
        try
        {
            TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of("Europe/Berlin")));

            ValueVector vv;

            vv = ValueVector.literalLong(1_680_193_853_072L, 1);
            assertEquals("2023-03-30T16:30:53.072", vv.getDateTime(0)
                    .toString());
            vv = ValueVector.literalAny(1, Instant.ofEpochMilli(1_680_193_853_072L)
                    .atZone(ZoneId.of("UTC")));
            assertEquals("2023-03-30T16:30:53.072", vv.getDateTime(0)
                    .toString());

            vv = ValueVector.literalString("2023-03-30T16:30:53.072", 1);
            assertEquals("2023-03-30T16:30:53.072", vv.getDateTime(0)
                    .toString());

            vv = ValueVector.literalDateTimeOffset(EpochDateTimeOffset.from(Instant.ofEpochMilli(1_680_193_853_072L)
                    .atZone(ZoneId.of("UTC"))), 1);
            assertEquals("2023-03-30T16:30:53.072", vv.getDateTime(0)
                    .toString());
        }
        finally
        {
            TimeZone.setDefault(defaultTimezone);
        }
    }

    @Test
    public void test_datetimeoffset_implicit_casts()
    {
        TimeZone defaultTimezone = TimeZone.getDefault();
        try
        {
            TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of("Europe/Berlin")));

            ValueVector vv;

            vv = ValueVector.literalLong(1_680_193_853_072L, 1);
            assertEquals("2023-03-30T16:30:53.072Z", vv.getDateTimeOffset(0)
                    .toString());
            vv = ValueVector.literalAny(1, Instant.ofEpochMilli(1_680_193_853_072L)
                    .atZone(ZoneId.of("UTC")));
            assertEquals("2023-03-30T16:30:53.072Z", vv.getDateTimeOffset(0)
                    .toString());

            vv = ValueVector.literalString("2023-03-30T16:30:53.072", 1);
            assertEquals("2023-03-30T16:30:53.072Z", vv.getDateTimeOffset(0)
                    .toString());

            vv = ValueVector.literalDateTime(EpochDateTime.from(1_680_193_853_072L), 1);
            assertEquals("2023-03-30T16:30:53.072Z", vv.getDateTimeOffset(0)
                    .toString());
        }
        finally
        {
            TimeZone.setDefault(defaultTimezone);
        }
    }

    @Test
    public void test_boolean_implicit_casts()
    {
        ValueVector vv;

        // Strings
        vv = ValueVector.literalString("y", 1);
        assertTrue(vv.getBoolean(0));
        vv = ValueVector.literalString("N", 1);
        assertFalse(vv.getBoolean(0));
        vv = ValueVector.literalString("yeS", 1);
        assertTrue(vv.getBoolean(0));
        vv = ValueVector.literalString("No", 1);
        assertFalse(vv.getBoolean(0));
        vv = ValueVector.literalString("TRUe", 1);
        assertTrue(vv.getBoolean(0));
        vv = ValueVector.literalString("FAlse", 1);
        assertFalse(vv.getBoolean(0));
        vv = ValueVector.literalString("1", 1);
        assertTrue(vv.getBoolean(0));
        vv = ValueVector.literalString("0", 1);
        assertFalse(vv.getBoolean(0));
        vv = ValueVector.literalString("nono", 1);
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
        vv = ValueVector.literalInt(10, 1);
        assertTrue(vv.getBoolean(0));
        vv = ValueVector.literalInt(0, 1);
        assertFalse(vv.getBoolean(0));
        // Long
        vv = ValueVector.literalLong(10, 1);
        assertTrue(vv.getBoolean(0));
        vv = ValueVector.literalLong(0, 1);
        assertFalse(vv.getBoolean(0));

        // Int (Any)
        vv = ValueVector.literalAny(1, 10);
        assertTrue(vv.getBoolean(0));
        vv = ValueVector.literalAny(1, 0);
        assertFalse(vv.getBoolean(0));
        // Long (Any)
        vv = ValueVector.literalAny(1, 10L);
        assertTrue(vv.getBoolean(0));
        vv = ValueVector.literalAny(1, 0L);
        assertFalse(vv.getBoolean(0));

        // Non supported
        vv = ValueVector.literalFloat(2F, 1);
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
        assertVectorsEquals(vv(ResolvedType.of(Type.String), "hello", "hello"), ValueVector.literalString("hello", 2));
        assertVectorsEquals(vv(ResolvedType.of(Type.Int), null, null), ValueVector.literalNull(ResolvedType.of(Type.Int), 2));
    }
}
