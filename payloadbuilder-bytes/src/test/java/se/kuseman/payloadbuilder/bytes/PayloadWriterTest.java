package se.kuseman.payloadbuilder.bytes;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import org.junit.Assert;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.ObjectVector;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Test of {@link PayloadWriter} */
public class PayloadWriterTest extends Assert
{
    @Test(
            expected = IllegalArgumentException.class)
    public void test_fail_any()
    {
        ValueVector v = vv(Type.Any, 1, 2, 3, 4);
        PayloadWriter.write(v);
    }

    @Test(
            expected = IllegalArgumentException.class)
    public void test_invalid_payload()
    {
        PayloadReader.read(new byte[] { 1, 2, 3 });
    }

    @Test(
            expected = IllegalArgumentException.class)
    public void test_invalid_payload_1()
    {
        PayloadReader.read(new byte[] { PayloadReader.P, PayloadReader.L, PayloadReader.B });
    }

    @Test(
            expected = IllegalArgumentException.class)
    public void test_invalid_payload_2()
    {
        PayloadReader.read(new byte[] { PayloadReader.P, PayloadReader.L, PayloadReader.B, 1, 2, 3 });
    }

    @Test(
            expected = IllegalArgumentException.class)
    public void test_invalid_payload_3()
    {
        PayloadReader.read(new byte[] { PayloadReader.P, PayloadReader.L, 1, 2, PayloadReader.B });
    }

    @Test
    public void test_array()
    {
        ValueVector v;
        ValueVector actual;
        byte[] bytes;

        // Empty
        v = VectorTestUtils.vv(ResolvedType.array(Type.Int));

        bytes = PayloadWriter.write(v);

        assertEquals(9, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        v = vv(ResolvedType.array(Type.Decimal), vv(Type.Decimal, 1692013080000L), vv(Type.Decimal, 1691013080000L), null, vv(Type.Decimal, 1690013080000L), vv(Type.Decimal, 1612013080000L),
                vv(Type.Decimal, 1672013080000L), vv(Type.Decimal, 1692013080000L), vv(Type.Decimal, 1622013080000L), vv(Type.Decimal, 1652013080000L));

        bytes = PayloadWriter.write(v);

        assertEquals(183, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // No nulls
        v = VectorTestUtils.vv(ResolvedType.array(Type.Int), vv(Type.Int, 1), vv(Type.Int, 2), vv(Type.Int, 3), vv(Type.Int, 4), vv(Type.Int, 5), vv(Type.Int, 6), vv(Type.Int, 7), vv(Type.Int, 8));

        bytes = PayloadWriter.write(v);

        assertEquals(107, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Literal null
        v = VectorTestUtils.vv(Type.Decimal, null, null, null, null, null);

        bytes = PayloadWriter.write(v);

        assertEquals(8, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Test array of arrays
        //@formatter:off
        v = VectorTestUtils.vv(ResolvedType.array(ResolvedType.array(Type.Int)),
                vv(ResolvedType.array(Type.Int), vv(Type.Int, 1), vv(Type.Int, 2)),
                vv(ResolvedType.array(Type.Int), vv(Type.Int, 3), vv(Type.Int, 4)),
                vv(ResolvedType.array(Type.Int), vv(Type.Int, 5), vv(Type.Int, 6)),
                vv(ResolvedType.array(Type.Int), vv(Type.Int, 7), vv(Type.Int, 8)));
        //@formatter:on

        bytes = PayloadWriter.write(v);

        assertEquals(140, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);
    }

    @Test
    public void test_decimal()
    {
        ValueVector v;
        ValueVector actual;
        byte[] bytes;

        // Empty
        v = VectorTestUtils.vv(Type.Decimal);

        bytes = PayloadWriter.write(v);

        assertEquals(8, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        v = VectorTestUtils.vv(Type.Decimal, 1692013080000L, 1691013080000L, null, 1690013080000L, 1612013080000L, 1672013080000L, 1692013080000L, 1622013080000L, 1652013080000L);

        bytes = PayloadWriter.write(v);

        assertEquals(118, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // No nulls
        v = VectorTestUtils.vv(Type.Decimal, 1692013080000L, 1691013080000L, 1690013080000L, 1612013080000L, 1672013080000L, 1692013080000L, 1622013080000L, 1652013080000L);

        bytes = PayloadWriter.write(v);

        assertEquals(112, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Literal
        v = VectorTestUtils.vv(Type.Decimal, 1692013080000L, 1692013080000L, 1692013080000L, 1692013080000L, 1692013080000L);

        bytes = PayloadWriter.write(v);

        assertEquals(24, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Literal null
        v = VectorTestUtils.vv(Type.Decimal, null, null, null, null, null);

        bytes = PayloadWriter.write(v);

        assertEquals(8, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);
    }

    @Test
    public void test_datetime()
    {
        ValueVector v;
        ValueVector actual;
        byte[] bytes;

        // Empty
        v = VectorTestUtils.vv(Type.DateTime);

        bytes = PayloadWriter.write(v);

        assertEquals(8, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        v = VectorTestUtils.vv(Type.DateTime, 1692013080000L, 1691013080000L, null, 1690013080000L, 1612013080000L, 1672013080000L, 1692013080000L, 1622013080000L, 1652013080000L);

        bytes = PayloadWriter.write(v);

        assertEquals(104, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // No nulls
        v = VectorTestUtils.vv(Type.DateTime, 1692013080000L, 1690013080000L, 1692013080000L, 1690013080000L, 1692013080000L);

        bytes = PayloadWriter.write(v);

        assertEquals(46, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Literal
        v = VectorTestUtils.vv(Type.DateTime, 1692013080000L, 1692013080000L, 1692013080000L, 1692013080000L, 1692013080000L);

        bytes = PayloadWriter.write(v);

        assertEquals(22, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Literal null
        v = VectorTestUtils.vv(Type.DateTime, null, null, null, null, null);

        bytes = PayloadWriter.write(v);

        assertEquals(8, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);
    }

    @Test
    public void test_int()
    {
        ValueVector v;
        ValueVector actual;
        byte[] bytes;

        // Empty
        v = VectorTestUtils.vv(Type.Int);

        bytes = PayloadWriter.write(v);

        assertEquals(8, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        v = VectorTestUtils.vv(Type.Int, 1, 2, null, 4, 5, 6, 7, 8, 9);

        bytes = PayloadWriter.write(v);

        assertEquals(48, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Literal
        v = VectorTestUtils.vv(Type.Int, 2, 2, 2, 2, 2);

        bytes = PayloadWriter.write(v);

        assertEquals(14, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Literal null
        v = VectorTestUtils.vv(Type.Int, null, null, null, null, null);

        bytes = PayloadWriter.write(v);

        assertEquals(8, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);
    }

    @Test
    public void test_long_cache()
    {
        ValueVector v;
        ValueVector actual;
        byte[] bytes;

        // Test without cache
        v = VectorTestUtils.vv(Type.Long, 1, 2, 3, 4, 5);

        bytes = PayloadWriter.write(v);

        // meta 5
        // type 1
        // length 1
        // nullCount 1
        // version 1
        // encoding 1
        // headers 5 * 4 = 20
        // 5 longs = 5 * 8 = 40
        // total: 70
        assertEquals(70, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Test cache
        v = VectorTestUtils.vv(Type.Long, 1, 2, 2, 2, 2);

        bytes = PayloadWriter.write(v);

        // meta 5
        // type 1
        // length 1
        // nullCount 1
        // version 1
        // encoding 1
        // headers 5 * 4 = 20
        // 2 longs = 2 * 8 = 16
        // total: 46
        assertEquals(46, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Test literal cache

        // @formatter:off
        Schema schema = Schema.of(
                Column.of("long", Column.Type.Long),
                Column.of("long1", Column.Type.Long));
        
        TupleVector tv = TupleVector.of(schema,
                asList(VectorTestUtils.vv(Type.Long, 1, 2),
                        VectorTestUtils.vv(Type.Long, 2, 2)));      // Long value 2 is cached in previous vector
        //@formatter:on

        bytes = PayloadWriter.write(ValueVector.literalTable(tv, 1));

        assertEquals(63, bytes.length);

        actual = PayloadReader.read(bytes);

        // @formatter:off
        Schema expectedSchema = Schema.of(
                Column.of("long_0", Column.Type.Long),
                Column.of("long_1", Column.Type.Long));
        
        TupleVector expected = TupleVector.of(expectedSchema,
                asList(VectorTestUtils.vv(Type.Long, 1, 2),
                        VectorTestUtils.vv(Type.Long, 2, 2)));
        //@formatter:on

        VectorTestUtils.assertTupleVectorsEquals(expected, actual.getTable(0));
    }

    @Test
    public void test_double_cache()
    {
        ValueVector v;
        ValueVector actual;
        byte[] bytes;

        // Test without cache
        v = VectorTestUtils.vv(Type.Double, 1, 2, 3, 4, 5);

        bytes = PayloadWriter.write(v);

        // meta 5
        // type 1
        // length 1
        // nullCount 1
        // version 1
        // encoding 1
        // headers 5 * 4 = 20
        // 5 doubles = 5 * 8 = 40
        // total: 70
        assertEquals(70, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Test cache
        v = VectorTestUtils.vv(Type.Double, 1, 2, 2, 2, 2);

        bytes = PayloadWriter.write(v);

        // meta 5
        // type 1
        // length 1
        // nullCount 1
        // version 1
        // encoding 1
        // headers 5 * 4 = 20
        // 2 doubles = 2 * 8 = 16
        // total: 46
        assertEquals(46, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Test literal cache

        // @formatter:off
        Schema schema = Schema.of(
                Column.of("long", Column.Type.Double),
                Column.of("long1", Column.Type.Double));
        
        TupleVector tv = TupleVector.of(schema,
                asList(VectorTestUtils.vv(Type.Double, 1, 2),
                        VectorTestUtils.vv(Type.Double, 2, 2)));      // Double value 2 is cached in previous vector
        //@formatter:on

        bytes = PayloadWriter.write(ValueVector.literalTable(tv, 1));

        assertEquals(63, bytes.length);

        actual = PayloadReader.read(bytes);

        // @formatter:off
        Schema expectedSchema = Schema.of(
                Column.of("double_0", Column.Type.Double),
                Column.of("double_1", Column.Type.Double));
        
        TupleVector expected = TupleVector.of(expectedSchema,
                asList(VectorTestUtils.vv(Type.Double, 1, 2),
                        VectorTestUtils.vv(Type.Double, 2, 2)));
        //@formatter:on

        VectorTestUtils.assertTupleVectorsEquals(expected, actual.getTable(0));
    }

    @Test
    public void test_long()
    {
        ValueVector v;
        ValueVector actual;
        byte[] bytes;

        // Empty
        v = VectorTestUtils.vv(Type.Long);

        bytes = PayloadWriter.write(v);

        assertEquals(8, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        v = VectorTestUtils.vv(Type.Long, 1, 2, null, 4, 5, 6, 7, 8, 9);

        bytes = PayloadWriter.write(v);

        assertEquals(112, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        v = VectorTestUtils.vv(Type.Long, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        bytes = PayloadWriter.write(v);

        assertEquals(118, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Literal
        v = VectorTestUtils.vv(Type.Long, 2, 2, 2, 2, 2);

        bytes = PayloadWriter.write(v);

        assertEquals(22, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Literal null
        v = VectorTestUtils.vv(Type.Long, null, null, null, null, null);

        bytes = PayloadWriter.write(v);

        assertEquals(8, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);
    }

    @Test
    public void test_string()
    {
        ValueVector v;
        ValueVector actual;
        byte[] bytes;

        // Empty
        v = VectorTestUtils.vv(Type.Long);

        bytes = PayloadWriter.write(v);

        assertEquals(8, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        v = VectorTestUtils.vv(Type.String, 1, "hello", null, 4, "hello", 6, 7, 8, "world");

        bytes = PayloadWriter.write(v);

        assertEquals(70, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        v = VectorTestUtils.vv(Type.String, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        bytes = PayloadWriter.write(v);

        assertEquals(64, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Literal
        v = VectorTestUtils.vv(Type.String, "sv", "sv", "sv", "sv");

        bytes = PayloadWriter.write(v);

        assertEquals(17, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Literal null
        v = VectorTestUtils.vv(Type.String, null, null, null, null, null);

        bytes = PayloadWriter.write(v);

        assertEquals(8, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);
    }

    @Test
    public void test_double()
    {
        ValueVector v;
        ValueVector actual;
        byte[] bytes;

        // Empty
        v = VectorTestUtils.vv(Type.Double);

        bytes = PayloadWriter.write(v);

        assertEquals(8, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        v = VectorTestUtils.vv(Type.Double, 1, 2, null, 4, 5, 6, 7, 8, 9);

        bytes = PayloadWriter.write(v);

        assertEquals(112, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        v = VectorTestUtils.vv(Type.Double, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        bytes = PayloadWriter.write(v);

        assertEquals(118, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Literal
        v = VectorTestUtils.vv(Type.Double, 2, 2, 2, 2, 2);

        bytes = PayloadWriter.write(v);

        assertEquals(22, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Literal null
        v = VectorTestUtils.vv(Type.Double, null, null, null, null, null);

        bytes = PayloadWriter.write(v);

        assertEquals(8, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);
    }

    @Test
    public void test_boolean()
    {
        ValueVector v;
        ValueVector actual;
        byte[] bytes;

        // Empty
        v = VectorTestUtils.vv(Type.Boolean);

        bytes = PayloadWriter.write(v);

        assertEquals(8, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        v = VectorTestUtils.vv(Type.Boolean, true, false, null, true, true, true, null, false, false);

        bytes = PayloadWriter.write(v);

        assertEquals(15, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        v = VectorTestUtils.vv(Type.Boolean, true, false, true, true, true, false, false);

        bytes = PayloadWriter.write(v);

        assertEquals(12, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Literal true
        v = VectorTestUtils.vv(Type.Boolean, true, true, true, true);

        bytes = PayloadWriter.write(v);

        assertEquals(11, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Literal false
        v = VectorTestUtils.vv(Type.Boolean, false, false, false, false);

        bytes = PayloadWriter.write(v);

        assertEquals(11, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Literal null
        v = VectorTestUtils.vv(Type.Boolean, null, null, null, null, null);

        bytes = PayloadWriter.write(v);

        assertEquals(8, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);
    }

    @Test
    public void test_float()
    {
        ValueVector v;
        ValueVector actual;
        byte[] bytes;

        // Empty
        v = VectorTestUtils.vv(Type.Float);

        bytes = PayloadWriter.write(v);

        assertEquals(8, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        v = VectorTestUtils.vv(Type.Float, 1, 2, null, 4, 5, 6, 7, 8, 9);

        bytes = PayloadWriter.write(v);

        assertEquals(48, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Literal
        v = VectorTestUtils.vv(Type.Float, 2, 2, 2, 2, 2);

        bytes = PayloadWriter.write(v);

        assertEquals(14, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Literal null
        v = VectorTestUtils.vv(Type.Float, null, null, null, null, null);

        bytes = PayloadWriter.write(v);

        assertEquals(8, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);
    }

    @Test
    public void test_table_with_array()
    {
        ValueVector v;
        byte[] bytes;
        TupleVector expected;
        Schema expectedSchema;

        // @formatter:off
        Schema schema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("float2", Column.Type.Float),
                Column.of("float3", Column.Type.Float),
                Column.of("array4", ResolvedType.array(Type.Int)));

        expected = TupleVector.of(schema,
                asList(VectorTestUtils.vv(Type.Int, 2, 2, 2, 2, 2),
                        VectorTestUtils.vv(Type.Float, 2.0F, 3.0F, null, 5.0F, 6.0F),
                        VectorTestUtils.vv(Type.Float, 33.0F, 33.0F, 33.0F, 33.0F, 33.0F),
                        ValueVector.literalNull(ResolvedType.of(Type.Float), 5),
                        vv(ResolvedType.array(Type.Int), vv(Type.Int, 1,2), vv(Type.Int, 3,4), vv(Type.Int, 5,6), vv(Type.Int, 7,8), vv(Type.Int, 9,10))
                        ));
        //@formatter:on

        v = ValueVector.literalTable(expected, 1);
        bytes = PayloadWriter.write(v);

        assertEquals(170, bytes.length);

        // Read with same schema

        TupleVector actual = PayloadReader.readTupleVector(bytes, schema, false);

        VectorTestUtils.assertTupleVectorsEquals(expected, actual);

        // Read with no schema

        actual = PayloadReader.readTupleVector(bytes, Schema.EMPTY, false);

        // @formatter:off
        expectedSchema = Schema.of(
                Column.of("int_0", Column.Type.Int),
                Column.of("float_1", Column.Type.Float),
                Column.of("float_2", Column.Type.Float),
                Column.of("float_3", Column.Type.Float),
                Column.of("array_4", ResolvedType.array(Type.Int)));

        expected = TupleVector.of(expectedSchema,
                asList(VectorTestUtils.vv(Type.Int, 2, 2, 2, 2, 2),
                        VectorTestUtils.vv(Type.Float, 2.0F, 3.0F, null, 5.0F, 6.0F),
                        VectorTestUtils.vv(Type.Float, 33.0F, 33.0F, 33.0F, 33.0F, 33.0F),
                        ValueVector.literalNull(ResolvedType.of(Type.Float), 5),
                        vv(ResolvedType.array(Type.Int), vv(Type.Int, 1,2), vv(Type.Int, 3,4), vv(Type.Int, 5,6), vv(Type.Int, 7,8), vv(Type.Int, 9,10))
                        ));
        //@formatter:on

        VectorTestUtils.assertTupleVectorsEquals(expected, actual);

        // Read with less columns schema no expand

        // @formatter:off
        schema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("float2", Column.Type.Float));
        //@formatter:on

        actual = PayloadReader.readTupleVector(bytes, schema, false);

        // @formatter:off
        expectedSchema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("float2", Column.Type.Float));

        expected = TupleVector.of(expectedSchema,
                asList(VectorTestUtils.vv(Type.Int, 2, 2, 2, 2, 2),
                        VectorTestUtils.vv(Type.Float, 2.0F, 3.0F, null, 5.0F, 6.0F),
                        VectorTestUtils.vv(Type.Float, 33.0F, 33.0F, 33.0F, 33.0F, 33.0F)
                        ));
        //@formatter:on

        VectorTestUtils.assertTupleVectorsEquals(expected, actual);

        // Read with less columns schema with expand

        // @formatter:off
        schema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("float2", Column.Type.Float));
        //@formatter:on

        actual = PayloadReader.readTupleVector(bytes, schema, true);

        // @formatter:off
        expectedSchema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("float2", Column.Type.Float),
                Column.of("float_3", Column.Type.Float),
                Column.of("array_4", ResolvedType.array(Type.Int)));

        expected = TupleVector.of(expectedSchema,
                asList(VectorTestUtils.vv(Type.Int, 2, 2, 2, 2, 2),
                        VectorTestUtils.vv(Type.Float, 2.0F, 3.0F, null, 5.0F, 6.0F),
                        VectorTestUtils.vv(Type.Float, 33.0F, 33.0F, 33.0F, 33.0F, 33.0F),
                        ValueVector.literalNull(ResolvedType.of(Type.Float), 5),
                        vv(ResolvedType.array(Type.Int), vv(Type.Int, 1,2), vv(Type.Int, 3,4), vv(Type.Int, 5,6), vv(Type.Int, 7,8), vv(Type.Int, 9,10))
                        ));
        //@formatter:on

        VectorTestUtils.assertTupleVectorsEquals(expected, actual);

        // Read with same schema with expand

        actual = PayloadReader.readTupleVector(bytes, expectedSchema, true);

        // @formatter:off
        expectedSchema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("float2", Column.Type.Float),
                Column.of("float_3", Column.Type.Float),
                Column.of("array_4", ResolvedType.array(Type.Int)));

        expected = TupleVector.of(expectedSchema,
                asList(VectorTestUtils.vv(Type.Int, 2, 2, 2, 2, 2),
                        VectorTestUtils.vv(Type.Float, 2.0F, 3.0F, null, 5.0F, 6.0F),
                        VectorTestUtils.vv(Type.Float, 33.0F, 33.0F, 33.0F, 33.0F, 33.0F),
                        ValueVector.literalNull(ResolvedType.of(Type.Float), 5),
                        vv(ResolvedType.array(Type.Int), vv(Type.Int, 1,2), vv(Type.Int, 3,4), vv(Type.Int, 5,6), vv(Type.Int, 7,8), vv(Type.Int, 9,10))
                        ));
        //@formatter:on

        VectorTestUtils.assertTupleVectorsEquals(expected, actual);
    }

    @Test
    public void test_table_context_schema()
    {
        ValueVector v;
        TupleVector actual;
        TupleVector expected;
        byte[] bytes;
        Schema expectedSchema;
        Schema expectedInnerSchema;

        // @formatter:off
        Schema schema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("float2", Column.Type.Float),
                Column.of("float3", Column.Type.Float));

        expected = TupleVector.of(schema,
                asList(VectorTestUtils.vv(Type.Int, 2, 2, 2, 2, 2),
                        VectorTestUtils.vv(Type.Float, 2.0F, 3.0F, null, 5.0F, 6.0F),
                        VectorTestUtils.vv(Type.Float, 33.0F, 33.0F, 33.0F, 33.0F, 33.0F),
                        ValueVector.literalNull(ResolvedType.of(Type.Float), 5)));
        //@formatter:on

        v = ValueVector.literalTable(expected, 1);
        bytes = PayloadWriter.write(v);

        assertEquals(80, bytes.length);

        actual = PayloadReader.readTupleVector(bytes, schema, false);
        VectorTestUtils.assertTupleVectorsEquals(expected, actual);

        // Test with less columns without expand

        // @formatter:off
        schema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("float2", Column.Type.Float));
        // @formatter:off

        actual = PayloadReader.readTupleVector(bytes, schema, false);

        // @formatter:off
        expected = TupleVector.of(schema,
                asList(VectorTestUtils.vv(Type.Int, 2, 2, 2, 2, 2),
                        VectorTestUtils.vv(Type.Float, 2.0F, 3.0F, null, 5.0F, 6.0F),
                        VectorTestUtils.vv(Type.Float, 33.0F, 33.0F, 33.0F, 33.0F, 33.0F)
                        // No last column here since the input schema doesn't have it and we don't expand
                        ));
        //@formatter:on

        VectorTestUtils.assertTupleVectorsEquals(expected, actual);

        // Test with more columns, and null is returned

        // @formatter:off
        schema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("float2", Column.Type.Float),
                Column.of("float3", Column.Type.Float),
                Column.of("boolean5", Column.Type.Boolean));
        // @formatter:off

        actual = PayloadReader.readTupleVector(bytes, schema, false);

        // @formatter:off
        expected = TupleVector.of(schema,
                asList(VectorTestUtils.vv(Type.Int, 2, 2, 2, 2, 2),
                        VectorTestUtils.vv(Type.Float, 2.0F, 3.0F, null, 5.0F, 6.0F),
                        VectorTestUtils.vv(Type.Float, 33.0F, 33.0F, 33.0F, 33.0F, 33.0F),
                        ValueVector.literalNull(ResolvedType.of(Type.Float), 5),
                        ValueVector.literalNull(ResolvedType.of(Type.Boolean), 5)
                        ));
        //@formatter:on

        VectorTestUtils.assertTupleVectorsEquals(expected, actual);

        // Test with less columns with expand

        // @formatter:off
        schema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("float2", Column.Type.Float));
        // @formatter:off

        actual = PayloadReader.readTupleVector(bytes, schema, true);

        // @formatter:off
        expectedSchema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("float2", Column.Type.Float),
                Column.of("float_3", Column.Type.Float));     // auto expanded column

        expected = TupleVector.of(expectedSchema,
                asList(VectorTestUtils.vv(Type.Int, 2, 2, 2, 2, 2),
                        VectorTestUtils.vv(Type.Float, 2.0F, 3.0F, null, 5.0F, 6.0F),
                        VectorTestUtils.vv(Type.Float, 33.0F, 33.0F, 33.0F, 33.0F, 33.0F),
                        ValueVector.literalNull(ResolvedType.of(Type.Float), 5)
                        ));
        //@formatter:on

        VectorTestUtils.assertTupleVectorsEquals(expected, actual);

        // Test nested table
        // @formatter:off
        Schema innerSchema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float)
                );

        schema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("float2", Column.Type.Float),
                Column.of("float3", Column.Type.Float),
                Column.of("table1", ResolvedType.table(innerSchema)),
                Column.of("object1", ResolvedType.object(innerSchema))
                );

        expected = TupleVector.of(schema,
                asList(VectorTestUtils.vv(Type.Int, 2, 2),
                        VectorTestUtils.vv(Type.Float, 2.0F, null),
                        VectorTestUtils.vv(Type.Float, 33.0F, 33.0F),
                        ValueVector.literalNull(ResolvedType.of(Type.Float), 2),
                        VectorTestUtils.vv(ResolvedType.table(innerSchema),
                                TupleVector.of(innerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 12, 22),
                                        VectorTestUtils.vv(Type.Float, 22.0F, 666.50F)
                                        )),
                                TupleVector.of(innerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 666, 666),
                                        VectorTestUtils.vv(Type.Float, null, 1234F)
                                        ))
                                ),
                        VectorTestUtils.vv(ResolvedType.object(innerSchema),
                                ObjectVector.wrap(TupleVector.of(innerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 12, 22),
                                        VectorTestUtils.vv(Type.Float, 22.0F, 666.50F)
                                        ))),
                                ObjectVector.wrap(TupleVector.of(innerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 666, 666),
                                        VectorTestUtils.vv(Type.Float, null, 1234F)
                                        )), 1)
                                )
                        ));
        //@formatter:on

        v = ValueVector.literalTable(expected, 1);
        bytes = PayloadWriter.write(v);

        assertEquals(198, bytes.length);

        actual = PayloadReader.readTupleVector(bytes, schema, false);

        VectorTestUtils.assertTupleVectorsEquals(expected, actual);

        // Test with no schema, all columns auto generated

        actual = PayloadReader.readTupleVector(bytes, Schema.EMPTY, true);

        //@formatter:off
        expectedInnerSchema = Schema.of(
                Column.of("int_0", Column.Type.Int),
                Column.of("float_1", Column.Type.Float)
                );

        expectedSchema = Schema.of(
                Column.of("int_0", Column.Type.Int),
                Column.of("float_1", Column.Type.Float),
                Column.of("float_2", Column.Type.Float),
                Column.of("float_3", Column.Type.Float),
                Column.of("table_4", ResolvedType.table(expectedInnerSchema)),
                Column.of("object_5", ResolvedType.object(expectedInnerSchema))
                );

        expected = TupleVector.of(expectedSchema,
                asList(VectorTestUtils.vv(Type.Int, 2, 2),
                        VectorTestUtils.vv(Type.Float, 2.0F, null),
                        VectorTestUtils.vv(Type.Float, 33.0F, 33.0F),
                        ValueVector.literalNull(ResolvedType.of(Type.Float), 2),
                        VectorTestUtils.vv(ResolvedType.table(expectedInnerSchema),
                                TupleVector.of(expectedInnerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 12, 22),
                                        VectorTestUtils.vv(Type.Float, 22.0F, 666.50F)
                                        )),
                                TupleVector.of(expectedInnerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 666, 666),
                                        VectorTestUtils.vv(Type.Float, null, 1234F)
                                        ))
                                ),
                        VectorTestUtils.vv(ResolvedType.object(expectedInnerSchema),
                                ObjectVector.wrap(TupleVector.of(expectedInnerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 12, 22),
                                        VectorTestUtils.vv(Type.Float, 22.0F, 666.50F)
                                        ))),
                                ObjectVector.wrap(TupleVector.of(expectedInnerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 666, 666),
                                        VectorTestUtils.vv(Type.Float, null, 1234F)
                                        )), 1)
                                )
                        ));
        //@formatter:on

        VectorTestUtils.assertTupleVectorsEquals(expected, actual);

        // One column short in nested object/table

        // @formatter:off
        innerSchema = Schema.of(
                Column.of("integer", Column.Type.Int)
                );

        schema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("float2", Column.Type.Float),
                Column.of("float3", Column.Type.Float),
                Column.of("table1", ResolvedType.table(innerSchema)),
                Column.of("object1", ResolvedType.object(innerSchema))
                );

        actual = PayloadReader.readTupleVector(bytes, schema, true);

        //@formatter:off
        expectedInnerSchema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float_1", Column.Type.Float)
                );

        expectedSchema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("float2", Column.Type.Float),
                Column.of("float3", Column.Type.Float),
                Column.of("table1", ResolvedType.table(expectedInnerSchema)),
                Column.of("object1", ResolvedType.object(expectedInnerSchema))
                );

        expected = TupleVector.of(expectedSchema,
                asList(VectorTestUtils.vv(Type.Int, 2, 2),
                        VectorTestUtils.vv(Type.Float, 2.0F, null),
                        VectorTestUtils.vv(Type.Float, 33.0F, 33.0F),
                        ValueVector.literalNull(ResolvedType.of(Type.Float), 2),
                        VectorTestUtils.vv(ResolvedType.table(expectedInnerSchema),
                                TupleVector.of(expectedInnerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 12, 22),
                                        VectorTestUtils.vv(Type.Float, 22.0F, 666.50F)
                                        )),
                                TupleVector.of(expectedInnerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 666, 666),
                                        VectorTestUtils.vv(Type.Float, null, 1234F)
                                        ))
                                ),
                        VectorTestUtils.vv(ResolvedType.object(expectedInnerSchema),
                                ObjectVector.wrap(TupleVector.of(expectedInnerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 12, 22),
                                        VectorTestUtils.vv(Type.Float, 22.0F, 666.50F)
                                        ))),
                                ObjectVector.wrap(TupleVector.of(expectedInnerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 666, 666),
                                        VectorTestUtils.vv(Type.Float, null, 1234F)
                                        )), 1)
                                )
                        ));
        //@formatter:on

        VectorTestUtils.assertTupleVectorsEquals(expected, actual);

        // More columns in inner schema than payload without expand

        // @formatter:off
        innerSchema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("decimal", Column.Type.Decimal)
                );

        schema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("float2", Column.Type.Float),
                Column.of("float3", Column.Type.Float),
                Column.of("table_4", ResolvedType.table(innerSchema)),
                Column.of("object_5", ResolvedType.object(innerSchema))
                );

        actual = PayloadReader.readTupleVector(bytes, schema, false);

        expected = TupleVector.of(schema,
                asList(VectorTestUtils.vv(Type.Int, 2, 2),
                        VectorTestUtils.vv(Type.Float, 2.0F, null),
                        VectorTestUtils.vv(Type.Float, 33.0F, 33.0F),
                        ValueVector.literalNull(ResolvedType.of(Type.Float), 2),
                        VectorTestUtils.vv(ResolvedType.table(innerSchema),
                                TupleVector.of(innerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 12, 22),
                                        VectorTestUtils.vv(Type.Float, 22.0F, 666.50F),
                                        ValueVector.literalNull(ResolvedType.of(Type.Decimal), 2)
                                        )),
                                TupleVector.of(innerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 666, 666),
                                        VectorTestUtils.vv(Type.Float, null, 1234F),
                                        ValueVector.literalNull(ResolvedType.of(Type.Decimal), 2)
                                        ))
                                ),
                        VectorTestUtils.vv(ResolvedType.object(innerSchema),
                                ObjectVector.wrap(TupleVector.of(innerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 12, 22),
                                        VectorTestUtils.vv(Type.Float, 22.0F, 666.50F),
                                        ValueVector.literalNull(ResolvedType.of(Type.Decimal), 2)
                                        ))),
                                ObjectVector.wrap(TupleVector.of(innerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 666, 666),
                                        VectorTestUtils.vv(Type.Float, null, 1234F),
                                        ValueVector.literalNull(ResolvedType.of(Type.Decimal), 2)
                                        )), 1)
                                )
                        ));
        //@formatter:on

        VectorTestUtils.assertTupleVectorsEquals(expected, actual);

        // More columns in inner schema than payload with expand

        // @formatter:off
        innerSchema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("decimal", Column.Type.Decimal)
                );

        schema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("float2", Column.Type.Float),
                Column.of("float3", Column.Type.Float),
                Column.of("table1", ResolvedType.table(innerSchema)),
                Column.of("object1", ResolvedType.object(innerSchema))
                );

        actual = PayloadReader.readTupleVector(bytes, schema, true);

        expected = TupleVector.of(schema,
                asList(VectorTestUtils.vv(Type.Int, 2, 2),
                        VectorTestUtils.vv(Type.Float, 2.0F, null),
                        VectorTestUtils.vv(Type.Float, 33.0F, 33.0F),
                        ValueVector.literalNull(ResolvedType.of(Type.Float), 2),
                        VectorTestUtils.vv(ResolvedType.table(innerSchema),
                                TupleVector.of(innerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 12, 22),
                                        VectorTestUtils.vv(Type.Float, 22.0F, 666.50F),
                                        ValueVector.literalNull(ResolvedType.of(Type.Decimal), 2)
                                        )),
                                TupleVector.of(innerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 666, 666),
                                        VectorTestUtils.vv(Type.Float, null, 1234F),
                                        ValueVector.literalNull(ResolvedType.of(Type.Decimal), 2)
                                        ))
                                ),
                        VectorTestUtils.vv(ResolvedType.object(innerSchema),
                                ObjectVector.wrap(TupleVector.of(innerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 12, 22),
                                        VectorTestUtils.vv(Type.Float, 22.0F, 666.50F),
                                        ValueVector.literalNull(ResolvedType.of(Type.Decimal), 2)
                                        ))),
                                ObjectVector.wrap(TupleVector.of(innerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 666, 666),
                                        VectorTestUtils.vv(Type.Float, null, 1234F),
                                        ValueVector.literalNull(ResolvedType.of(Type.Decimal), 2)
                                        )), 1)
                                )
                        ));
        //@formatter:on

        VectorTestUtils.assertTupleVectorsEquals(expected, actual);

        // Missing whole table and object in provided schema

        schema = Schema.of(Column.of("integer", Column.Type.Int), Column.of("float", Column.Type.Float), Column.of("float2", Column.Type.Float), Column.of("float3", Column.Type.Float));

        actual = PayloadReader.readTupleVector(bytes, schema, true);

        //@formatter:off
        expectedInnerSchema = Schema.of(
                Column.of("int_0", Column.Type.Int),
                Column.of("float_1", Column.Type.Float)
                );

        expectedSchema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("float2", Column.Type.Float),
                Column.of("float3", Column.Type.Float),
                Column.of("table_4", ResolvedType.table(expectedInnerSchema)),
                Column.of("object_5", ResolvedType.object(expectedInnerSchema))
                );

        expected = TupleVector.of(expectedSchema,
                asList(VectorTestUtils.vv(Type.Int, 2, 2),
                        VectorTestUtils.vv(Type.Float, 2.0F, null),
                        VectorTestUtils.vv(Type.Float, 33.0F, 33.0F),
                        ValueVector.literalNull(ResolvedType.of(Type.Float), 2),
                        VectorTestUtils.vv(ResolvedType.table(expectedInnerSchema),
                                TupleVector.of(expectedInnerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 12, 22),
                                        VectorTestUtils.vv(Type.Float, 22.0F, 666.50F)
                                        )),
                                TupleVector.of(expectedInnerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 666, 666),
                                        VectorTestUtils.vv(Type.Float, null, 1234F)
                                        ))
                                ),
                        VectorTestUtils.vv(ResolvedType.object(expectedInnerSchema),
                                ObjectVector.wrap(TupleVector.of(expectedInnerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 12, 22),
                                        VectorTestUtils.vv(Type.Float, 22.0F, 666.50F)
                                        ))),
                                ObjectVector.wrap(TupleVector.of(expectedInnerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 666, 666),
                                        VectorTestUtils.vv(Type.Float, null, 1234F)
                                        )), 1)
                                )
                        ));
        //@formatter:on

        VectorTestUtils.assertTupleVectorsEquals(expected, actual);
    }

    @Test
    public void test_table()
    {
        ValueVector v;
        ValueVector actual;
        byte[] bytes;
        Schema expectedSchema;
        TupleVector expected;

        // @formatter:off
        Schema schema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("float2", Column.Type.Float),
                Column.of("float3", Column.Type.Float));

        expectedSchema = Schema.of(
                Column.of("int_0", Column.Type.Int),
                Column.of("float_1", Column.Type.Float),
                Column.of("float_2", Column.Type.Float),
                Column.of("float_3", Column.Type.Float));

        TupleVector tv = TupleVector.of(schema,
                asList(VectorTestUtils.vv(Type.Int, 2, 2, 2, 2, 2),
                        VectorTestUtils.vv(Type.Float, 2.0F, 3.0F, null, 5.0F, 6.0F),
                        VectorTestUtils.vv(Type.Float, 33.0F, 33.0F, 33.0F, 33.0F, 33.0F),
                        ValueVector.literalNull(ResolvedType.of(Type.Float), 5)));
        //@formatter:on

        // Empty
        v = VectorTestUtils.vv(ResolvedType.table(schema));

        bytes = PayloadWriter.write(v);

        assertEquals(13, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(VectorTestUtils.vv(ResolvedType.table(expectedSchema)), actual);

        v = ValueVector.literalTable(tv, 1);
        bytes = PayloadWriter.write(v);

        assertEquals(80, bytes.length);

        actual = PayloadReader.read(bytes);

        // @formatter:off
        expectedSchema = Schema.of(
                Column.of("int_0", Column.Type.Int),
                Column.of("float_1", Column.Type.Float),
                Column.of("float_2", Column.Type.Float),
                Column.of("float_3", Column.Type.Float));

        expected = TupleVector.of(expectedSchema,
                asList(VectorTestUtils.vv(Type.Int, 2, 2, 2, 2, 2),
                        VectorTestUtils.vv(Type.Float, 2.0F, 3.0F, null, 5.0F, 6.0F),
                        VectorTestUtils.vv(Type.Float, 33.0F, 33.0F, 33.0F, 33.0F, 33.0F),
                        ValueVector.literalNull(ResolvedType.of(Type.Float), 5)));
        //@formatter:on

        VectorTestUtils.assertTupleVectorsEquals(expected, actual.getTable(0));
    }

    @Test
    public void test_object()
    {
        ValueVector v;
        ValueVector actual;
        byte[] bytes;
        Schema expectedSchema;
        TupleVector expected;

        // @formatter:off
        Schema schema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("float2", Column.Type.Float),
                Column.of("float3", Column.Type.Float));

        expectedSchema = Schema.of(
                Column.of("int_0", Column.Type.Int),
                Column.of("float_1", Column.Type.Float),
                Column.of("float_2", Column.Type.Float),
                Column.of("float_3", Column.Type.Float));

        TupleVector tv = TupleVector.of(schema,
                asList(VectorTestUtils.vv(Type.Int, 2, 2, 2, 2, 2),
                        VectorTestUtils.vv(Type.Float, 2.0F, 3.0F, null, 5.0F, 6.0F),
                        VectorTestUtils.vv(Type.Float, 33.0F, 33.0F, 33.0F, 33.0F, 33.0F),
                        ValueVector.literalNull(ResolvedType.of(Type.Float), 5)));

        ObjectVector ov = ObjectVector.wrap(tv, 2);
        //@formatter:on

        // Empty
        v = VectorTestUtils.vv(ResolvedType.object(schema));

        bytes = PayloadWriter.write(v);

        assertEquals(13, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(VectorTestUtils.vv(ResolvedType.object(expectedSchema)), actual);

        v = ValueVector.literalObject(ov, 1);
        bytes = PayloadWriter.write(v);

        assertEquals(53, bytes.length);

        actual = PayloadReader.read(bytes);

        // @formatter:off
        expectedSchema = Schema.of(
                Column.of("int_0", Column.Type.Int),
                Column.of("float_1", Column.Type.Float),
                Column.of("float_2", Column.Type.Float),
                Column.of("float_3", Column.Type.Float));

        expected = TupleVector.of(expectedSchema,
                asList(VectorTestUtils.vv(Type.Int, 2),
                        VectorTestUtils.vv(Type.Float, (Float) null),
                        VectorTestUtils.vv(Type.Float, 33.0F),
                        ValueVector.literalNull(ResolvedType.of(Type.Float), 1)));
        //@formatter:on

        VectorTestUtils.assertVectorsEquals(ValueVector.literalObject(ObjectVector.wrap(expected), 1), actual);
    }

    @Test
    public void test_nested_table()
    {
        ValueVector v;
        ValueVector actual;
        byte[] bytes;
        Schema expectedSchema;
        TupleVector expected;

        // @formatter:off
        Schema innerSchema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float)
                );

        Schema schema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("float2", Column.Type.Float),
                Column.of("float3", Column.Type.Float),
                Column.of("table1", ResolvedType.table(innerSchema)),
                Column.of("object1", ResolvedType.object(innerSchema))
                );

        TupleVector tv = TupleVector.of(schema,
                asList(VectorTestUtils.vv(Type.Int, 2, 2),
                        VectorTestUtils.vv(Type.Float, 2.0F, null),
                        VectorTestUtils.vv(Type.Float, 33.0F, 33.0F),
                        ValueVector.literalNull(ResolvedType.of(Type.Float), 2),
                        VectorTestUtils.vv(ResolvedType.table(innerSchema), 
                                TupleVector.of(innerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 12, 22),
                                        VectorTestUtils.vv(Type.Float, 22.0F, 666.50F)
                                        )),
                                TupleVector.of(innerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 666, 666),
                                        VectorTestUtils.vv(Type.Float, null, 1234F)
                                        ))
                                ),
                        VectorTestUtils.vv(ResolvedType.object(innerSchema),
                                ObjectVector.wrap(TupleVector.of(innerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 12, 22),
                                        VectorTestUtils.vv(Type.Float, 22.0F, 666.50F)
                                        ))),
                                ObjectVector.wrap(TupleVector.of(innerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 666, 123, 666),
                                        VectorTestUtils.vv(Type.Float, null, 456, 1234F)
                                        )), 2)
                                )
                        ));
        //@formatter:on

        v = ValueVector.literalTable(tv, 1);
        bytes = PayloadWriter.write(v);

        assertEquals(198, bytes.length);
        actual = PayloadReader.read(bytes);

        // @formatter:off
        Schema expectedInnerSchema = Schema.of(
                Column.of("int_0", Column.Type.Int),
                Column.of("float_1", Column.Type.Float)
                );

        expectedSchema = Schema.of(
                Column.of("int_0", Column.Type.Int),
                Column.of("float_1", Column.Type.Float),
                Column.of("float_2", Column.Type.Float),
                Column.of("float_3", Column.Type.Float),
                Column.of("table_4", ResolvedType.table(expectedInnerSchema)),
                Column.of("object_5", ResolvedType.object(expectedInnerSchema)));

        expected = TupleVector.of(expectedSchema,
                asList(VectorTestUtils.vv(Type.Int, 2, 2),
                        VectorTestUtils.vv(Type.Float, 2.0F, null),
                        VectorTestUtils.vv(Type.Float, 33.0F, 33.0F),
                        ValueVector.literalNull(ResolvedType.of(Type.Float), 2),
                        VectorTestUtils.vv(ResolvedType.table(expectedInnerSchema),
                                TupleVector.of(expectedInnerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 12, 22),
                                        VectorTestUtils.vv(Type.Float, 22.0F, 666.50F)
                                        )),
                                TupleVector.of(expectedInnerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 666, 666),
                                        VectorTestUtils.vv(Type.Float, null, 1234F)
                                        ))
                                ),
                        VectorTestUtils.vv(ResolvedType.object(expectedInnerSchema),
                                ObjectVector.wrap(TupleVector.of(expectedInnerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 12),
                                        VectorTestUtils.vv(Type.Float, 22.0F)
                                        ))),
                                ObjectVector.wrap(TupleVector.of(expectedInnerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 666),
                                        VectorTestUtils.vv(Type.Float, 1234F)
                                        )))
                                )
                        ));
        //@formatter:on
        VectorTestUtils.assertTupleVectorsEquals(expected, actual.getTable(0));
    }
}
