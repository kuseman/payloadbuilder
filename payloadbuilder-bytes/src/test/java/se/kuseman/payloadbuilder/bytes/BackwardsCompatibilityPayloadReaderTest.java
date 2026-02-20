package se.kuseman.payloadbuilder.bytes;

import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.math.BigDecimal;
import java.util.List;

import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.EpochDateTime;
import se.kuseman.payloadbuilder.api.execution.ObjectVector;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/**
 * Test that aims to verify that we dont break backwards compatibility when changing code in write parts. We keep byte sequences from all previous changes and verifies that those still holds.
 */
class BackwardsCompatibilityPayloadReaderTest
{
    @Test
    void test_intvector_1()
    {
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(Type.Int, 1,1,1))));
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(Type.Int, null,null,null))));
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(Type.Int, 1,null,3))));
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(Type.Int, 1,2,3))));
        // {80,76,66,2,1,3,0,1,0,0,0,0,1,1}
        // {80,76,66,2,1,3,3,0}
        // {80,76,66,2,1,3,1,2,1,1,0,0,0,1,0,0,0,0,0,0,0,3,2}
        // {80,76,66,2,1,3,0,1,1,0,0,0,1,0,0,0,2,0,0,0,3,1}
        assertVectorsEquals(vv(Column.Type.Int, 1, 1, 1), PayloadReader.read(new byte[] { 80, 76, 66, 2, 1, 3, 0, 1, 0, 0, 0, 0, 1, 1 }));
        assertVectorsEquals(vv(Column.Type.Int, null, null, null), PayloadReader.read(new byte[] { 80, 76, 66, 2, 1, 3, 3, 0 }));
        assertVectorsEquals(vv(Column.Type.Int, 1, null, 3), PayloadReader.read(new byte[] { 80, 76, 66, 2, 1, 3, 1, 2, 1, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 3, 2 }));
        assertVectorsEquals(vv(Column.Type.Int, 1, 2, 3), PayloadReader.read(new byte[] { 80, 76, 66, 2, 1, 3, 0, 1, 1, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 1 }));
    }

    @Test
    void test_longvector_1()
    {
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(Type.Long, 1,1,1))));
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(Type.Long, null,null,null))));
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(Type.Long, 1,null,3))));
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(Type.Long, 1,2,3))));
        // {80,76,66,2,2,3,0,1,0,0,0,0,13,0,0,0,0,0,0,0,1,1}
        // {80,76,66,2,2,3,3,0}
        // {80,76,66,2,2,3,1,2,1,1,0,0,0,22,0,0,0,0,0,0,0,30,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,3,2}
        // {80,76,66,2,2,3,0,1,1,0,0,0,21,0,0,0,29,0,0,0,37,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,2,0,0,0,0,0,0,0,3,1}
        assertVectorsEquals(vv(Column.Type.Long, 1, 1, 1), PayloadReader.read(new byte[] { 80, 76, 66, 2, 2, 3, 0, 1, 0, 0, 0, 0, 13, 0, 0, 0, 0, 0, 0, 0, 1, 1 }));
        assertVectorsEquals(vv(Column.Type.Long, null, null, null), PayloadReader.read(new byte[] { 80, 76, 66, 2, 2, 3, 3, 0 }));
        assertVectorsEquals(vv(Column.Type.Long, 1, null, 3),
                PayloadReader.read(new byte[] { 80, 76, 66, 2, 2, 3, 1, 2, 1, 1, 0, 0, 0, 22, 0, 0, 0, 0, 0, 0, 0, 30, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 3, 2 }));
        assertVectorsEquals(vv(Column.Type.Long, 1, 2, 3),
                PayloadReader.read(new byte[] { 80, 76, 66, 2, 2, 3, 0, 1, 1, 0, 0, 0, 21, 0, 0, 0, 29, 0, 0, 0, 37, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 1 }));
    }

    @Test
    void test_floatvector_1()
    {
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(Type.Float, 1.11f,1.11f,1.11f))));
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(Type.Float, null,null,null))));
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(Type.Float, 1.11f,null,3.33f))));
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(Type.Float, 1.11f,2.22f,3.33f))));
        // {80,76,66,2,3,3,0,1,0,63,-114,20,123,1}
        // {80,76,66,2,3,3,3,0}
        // {80,76,66,2,3,3,1,2,1,1,63,-114,20,123,0,0,0,0,64,85,30,-72,2}
        // {80,76,66,2,3,3,0,1,1,63,-114,20,123,64,14,20,123,64,85,30,-72,1}
        assertVectorsEquals(vv(Column.Type.Float, 1.11f, 1.11f, 1.11f), PayloadReader.read(new byte[] { 80, 76, 66, 2, 3, 3, 0, 1, 0, 63, -114, 20, 123, 1 }));
        assertVectorsEquals(vv(Column.Type.Float, null, null, null), PayloadReader.read(new byte[] { 80, 76, 66, 2, 3, 3, 3, 0 }));
        assertVectorsEquals(vv(Column.Type.Float, 1.11f, null, 3.33f), PayloadReader.read(new byte[] { 80, 76, 66, 2, 3, 3, 1, 2, 1, 1, 63, -114, 20, 123, 0, 0, 0, 0, 64, 85, 30, -72, 2 }));
        assertVectorsEquals(vv(Column.Type.Float, 1.11f, 2.22f, 3.33f), PayloadReader.read(new byte[] { 80, 76, 66, 2, 3, 3, 0, 1, 1, 63, -114, 20, 123, 64, 14, 20, 123, 64, 85, 30, -72, 1 }));
    }

    @Test
    void test_doublevector_1()
    {
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(Type.Double, 1.11d,1.11d,1.11d))));
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(Type.Double, null,null,null))));
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(Type.Double, 1.11d,null,3.33d))));
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(Type.Double, 1.11d,2.22d,3.33d))));
        // {80,76,66,2,4,3,0,1,0,0,0,0,13,63,-15,-62,-113,92,40,-11,-61,1}
        // {80,76,66,2,4,3,3,0}
        // {80,76,66,2,4,3,1,2,1,1,0,0,0,22,0,0,0,0,0,0,0,30,63,-15,-62,-113,92,40,-11,-61,64,10,-93,-41,10,61,112,-92,2}
        // {80,76,66,2,4,3,0,1,1,0,0,0,21,0,0,0,29,0,0,0,37,63,-15,-62,-113,92,40,-11,-61,64,1,-62,-113,92,40,-11,-61,64,10,-93,-41,10,61,112,-92,1}
        assertVectorsEquals(vv(Column.Type.Double, 1.11d, 1.11d, 1.11d), PayloadReader.read(new byte[] { 80, 76, 66, 2, 4, 3, 0, 1, 0, 0, 0, 0, 13, 63, -15, -62, -113, 92, 40, -11, -61, 1 }));
        assertVectorsEquals(vv(Column.Type.Double, null, null, null), PayloadReader.read(new byte[] { 80, 76, 66, 2, 4, 3, 3, 0 }));
        assertVectorsEquals(vv(Column.Type.Double, 1.11d, null, 3.33d),
                PayloadReader.read(new byte[] { 80, 76, 66, 2, 4, 3, 1, 2, 1, 1, 0, 0, 0, 22, 0, 0, 0, 0, 0, 0, 0, 30, 63, -15, -62, -113, 92, 40, -11, -61, 64, 10, -93, -41, 10, 61, 112, -92, 2 }));
        assertVectorsEquals(vv(Column.Type.Double, 1.11d, 2.22d, 3.33d),
                PayloadReader.read(new byte[] {
                        80, 76, 66, 2, 4, 3, 0, 1, 1, 0, 0, 0, 21, 0, 0, 0, 29, 0, 0, 0, 37, 63, -15, -62, -113, 92, 40, -11, -61, 64, 1, -62, -113, 92, 40, -11, -61, 64, 10, -93, -41, 10, 61, 112,
                        -92, 1 }));
    }

    @Test
    void test_booleanvector_1()
    {
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(Type.Boolean, true,true,true))));
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(Type.Boolean, null,null,null))));
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(Type.Boolean, true,null,false))));
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(Type.Boolean, true,false,true))));
        // {80,76,66,2,0,3,0,1,0,1,1}
        // {80,76,66,2,0,3,3,0}
        // {80,76,66,2,0,3,1,2,1,1,1,1,2}
        // {80,76,66,2,0,3,0,1,1,1,5,1}
        assertVectorsEquals(vv(Column.Type.Boolean, true, true, true), PayloadReader.read(new byte[] { 80, 76, 66, 2, 0, 3, 0, 1, 0, 1, 1 }));
        assertVectorsEquals(vv(Column.Type.Boolean, null, null, null), PayloadReader.read(new byte[] { 80, 76, 66, 2, 0, 3, 3, 0 }));
        assertVectorsEquals(vv(Column.Type.Boolean, true, null, false), PayloadReader.read(new byte[] { 80, 76, 66, 2, 0, 3, 1, 2, 1, 1, 1, 1, 2 }));
        assertVectorsEquals(vv(Column.Type.Boolean, true, false, true), PayloadReader.read(new byte[] { 80, 76, 66, 2, 0, 3, 0, 1, 1, 1, 5, 1 }));
    }

    @Test
    void test_stringvector_1()
    {
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(Type.String, "sv", "sv", "sv"))));
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(Type.String, null,null,null))));
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(Type.String, "sv",null,"da"))));
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(Type.String, "sv","da","fi"))));
        // {80,76,66,2,5,3,0,1,0,0,0,0,13,2,115,118,1}
        // {80,76,66,2,5,3,3,0}
        // {80,76,66,2,5,3,1,2,1,1,0,0,0,22,0,0,0,0,0,0,0,25,2,115,118,2,100,97,2}
        // {80,76,66,2,5,3,0,1,1,0,0,0,21,0,0,0,24,0,0,0,27,2,115,118,2,100,97,2,102,105,1}
        assertVectorsEquals(vv(Column.Type.String, "sv", "sv", "sv"), PayloadReader.read(new byte[] { 80, 76, 66, 2, 5, 3, 0, 1, 0, 0, 0, 0, 13, 2, 115, 118, 1 }));
        assertVectorsEquals(vv(Column.Type.String, null, null, null), PayloadReader.read(new byte[] { 80, 76, 66, 2, 5, 3, 3, 0 }));
        assertVectorsEquals(vv(Column.Type.String, "sv", null, "da"),
                PayloadReader.read(new byte[] { 80, 76, 66, 2, 5, 3, 1, 2, 1, 1, 0, 0, 0, 22, 0, 0, 0, 0, 0, 0, 0, 25, 2, 115, 118, 2, 100, 97, 2 }));
        assertVectorsEquals(vv(Column.Type.String, "sv", "da", "fi"),
                PayloadReader.read(new byte[] { 80, 76, 66, 2, 5, 3, 0, 1, 1, 0, 0, 0, 21, 0, 0, 0, 24, 0, 0, 0, 27, 2, 115, 118, 2, 100, 97, 2, 102, 105, 1 }));
    }

    @Test
    void test_stringvector_multibyte_utf8_1()
    {
        // Pin for multi-byte (2/3-byte cyrillic, 3-byte CJK) UTF-8 characters, as opposed to test_stringvector_1's ASCII-only values
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(Type.String, "one", "two", "Между", "Зареги", "五"))));
        // { 80, 76, 66, 2, 5, 5, 0, 1, 1, 0, 0, 0, 29, 0, 0, 0, 33, 0, 0, 0, 37, 0, 0, 0, 48, 0, 0, 0, 61, 3, 111, 110, 101, 3, 116, 119, 111, 10, -48, -100, -48, -75, -48, -74, -48, -76, -47, -125,
        // 12, -48, -105, -48, -80, -47, -128, -48, -75, -48, -77, -48, -72, 3, -28, -70, -108, 1 }
        assertVectorsEquals(vv(Column.Type.String, "one", "two", "Между", "Зареги", "五"),
                PayloadReader.read(new byte[] {
                        80, 76, 66, 2, 5, 5, 0, 1, 1, 0, 0, 0, 29, 0, 0, 0, 33, 0, 0, 0, 37, 0, 0, 0, 48, 0, 0, 0, 61, 3, 111, 110, 101, 3, 116, 119, 111, 10, -48, -100, -48, -75, -48, -74, -48, -76,
                        -47, -125, 12, -48, -105, -48, -80, -47, -128, -48, -75, -48, -77, -48, -72, 3, -28, -70, -108, 1 }));
    }

    @Test
    void test_decimalvector_1()
    {
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(Type.Decimal, new BigDecimal("10.10"), new BigDecimal("10.10"), new BigDecimal("10.10")))));
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(Type.Decimal, null,null,null))));
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(Type.Decimal, new BigDecimal("10.10"), null, new BigDecimal("30.30")))));
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(Type.Decimal, new BigDecimal("10.10"),new BigDecimal("20.20"),new BigDecimal("30.30")))));
        // {80,76,66,2,7,3,0,1,0,0,0,0,13,2,3,-14,2,1}
        // {80,76,66,2,7,3,3,0}
        // {80,76,66,2,7,3,1,2,1,1,0,0,0,22,0,0,0,0,0,0,0,26,2,3,-14,2,2,11,-42,2,2}
        // {80,76,66,2,7,3,0,1,1,0,0,0,21,0,0,0,25,0,0,0,29,2,3,-14,2,2,7,-28,2,2,11,-42,2,1}
        assertVectorsEquals(vv(Column.Type.Decimal, new BigDecimal("10.10"), new BigDecimal("10.10"), new BigDecimal("10.10")),
                PayloadReader.read(new byte[] { 80, 76, 66, 2, 7, 3, 0, 1, 0, 0, 0, 0, 13, 2, 3, -14, 2, 1 }));
        assertVectorsEquals(vv(Column.Type.Decimal, null, null, null), PayloadReader.read(new byte[] { 80, 76, 66, 2, 7, 3, 3, 0 }));
        assertVectorsEquals(vv(Column.Type.Decimal, new BigDecimal("10.10"), null, new BigDecimal("30.30")),
                PayloadReader.read(new byte[] { 80, 76, 66, 2, 7, 3, 1, 2, 1, 1, 0, 0, 0, 22, 0, 0, 0, 0, 0, 0, 0, 26, 2, 3, -14, 2, 2, 11, -42, 2, 2 }));
        assertVectorsEquals(vv(Column.Type.Decimal, new BigDecimal("10.10"), new BigDecimal("20.20"), new BigDecimal("30.30")),
                PayloadReader.read(new byte[] { 80, 76, 66, 2, 7, 3, 0, 1, 1, 0, 0, 0, 21, 0, 0, 0, 25, 0, 0, 0, 29, 2, 3, -14, 2, 2, 7, -28, 2, 2, 11, -42, 2, 1 }));
    }

    @Test
    void test_datetimevector_1()
    {
        // long epoch = 1772178000345L;
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(Type.DateTime, EpochDateTime.from(epoch), EpochDateTime.from(epoch), EpochDateTime.from(epoch)))));
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(Type.DateTime, null,null,null))));
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(Type.DateTime, EpochDateTime.from(epoch), null, EpochDateTime.from(epoch + 20000)))));
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(Type.DateTime, EpochDateTime.from(epoch),EpochDateTime.from(epoch + 10000),EpochDateTime.from(epoch +
        // 20000)))));
        // {80,76,66,2,6,3,0,1,0,0,0,0,13,0,0,1,-100,-98,10,73,-39,1}
        // {80,76,66,2,6,3,3,0}
        // {80,76,66,2,6,3,1,2,1,1,0,0,0,22,0,0,0,0,0,0,0,30,0,0,1,-100,-98,10,73,-39,0,0,1,-100,-98,10,-105,-7,2}
        // {80,76,66,2,6,3,0,1,1,0,0,0,21,0,0,0,29,0,0,0,37,0,0,1,-100,-98,10,73,-39,0,0,1,-100,-98,10,112,-23,0,0,1,-100,-98,10,-105,-7,1}

        long epoch = 1772178000345L;
        assertVectorsEquals(vv(Column.Type.DateTime, EpochDateTime.from(epoch), EpochDateTime.from(epoch), EpochDateTime.from(epoch)),
                PayloadReader.read(new byte[] { 80, 76, 66, 2, 6, 3, 0, 1, 0, 0, 0, 0, 13, 0, 0, 1, -100, -98, 10, 73, -39, 1 }));
        assertVectorsEquals(vv(Column.Type.DateTime, null, null, null), PayloadReader.read(new byte[] { 80, 76, 66, 2, 6, 3, 3, 0 }));
        assertVectorsEquals(vv(Column.Type.DateTime, EpochDateTime.from(epoch), null, EpochDateTime.from(epoch + 20000)),
                PayloadReader.read(new byte[] { 80, 76, 66, 2, 6, 3, 1, 2, 1, 1, 0, 0, 0, 22, 0, 0, 0, 0, 0, 0, 0, 30, 0, 0, 1, -100, -98, 10, 73, -39, 0, 0, 1, -100, -98, 10, -105, -7, 2 }));
        assertVectorsEquals(vv(Column.Type.DateTime, EpochDateTime.from(epoch), EpochDateTime.from(epoch + 10000), EpochDateTime.from(epoch + 20000)), PayloadReader.read(new byte[] {
                80, 76, 66, 2, 6, 3, 0, 1, 1, 0, 0, 0, 21, 0, 0, 0, 29, 0, 0, 0, 37, 0, 0, 1, -100, -98, 10, 73, -39, 0, 0, 1, -100, -98, 10, 112, -23, 0, 0, 1, -100, -98, 10, -105, -7, 1 }));
    }

    @Test
    void test_arrayvector_1()
    {
        // Schema.of(Column.of("array", ResolvedType.array(Type.Int)))
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(ResolvedType.array(Type.Int), vv(Type.Int, 1, 2), vv(Type.Int, 3, 4, 5), vv(Type.Int)))));
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(ResolvedType.array(Type.Int), null, null, null))));
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(ResolvedType.array(Type.Int), vv(Type.Int, 1, 2), null, vv(Type.Int, 3)))));
        // { 80, 76, 66, 2, 8, 1, 3, 0, 1, 1, 0, 0, 0, 22, 0, 0, 0, 34, 0, 0, 0, 50, 2, 0, 1, 1, 0, 0, 0, 1, 0, 0, 0, 2, 3, 0, 1, 1, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 5, 0, 0, 0 }
        // { 80, 76, 66, 2, 8, 1, 3, 3, 3 }
        // { 80, 76, 66, 2, 8, 1, 3, 1, 2, 1, 1, 0, 0, 0, 23, 0, 0, 0, 0, 0, 0, 0, 35, 2, 0, 1, 1, 0, 0, 0, 1, 0, 0, 0, 2, 1, 0, 1, 0, 0, 0, 0, 3, 1 }
        assertVectorsEquals(vv(ResolvedType.array(Type.Int), vv(Type.Int, 1, 2), vv(Type.Int, 3, 4, 5), vv(Type.Int)), PayloadReader.read(
                new byte[] { 80, 76, 66, 2, 8, 1, 3, 0, 1, 1, 0, 0, 0, 22, 0, 0, 0, 34, 0, 0, 0, 50, 2, 0, 1, 1, 0, 0, 0, 1, 0, 0, 0, 2, 3, 0, 1, 1, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 5, 0, 0, 0 }));
        assertVectorsEquals(vv(ResolvedType.array(Type.Int), null, null, null), PayloadReader.read(new byte[] { 80, 76, 66, 2, 8, 1, 3, 3, 3 }));
        assertVectorsEquals(vv(ResolvedType.array(Type.Int), vv(Type.Int, 1, 2), null, vv(Type.Int, 3)),
                PayloadReader.read(new byte[] { 80, 76, 66, 2, 8, 1, 3, 1, 2, 1, 1, 0, 0, 0, 23, 0, 0, 0, 0, 0, 0, 0, 35, 2, 0, 1, 1, 0, 0, 0, 1, 0, 0, 0, 2, 1, 0, 1, 0, 0, 0, 0, 3, 1 }));
    }

    @Test
    void test_arrayvector_nested_1()
    {
        // Array of array (two levels deep) - Schema.of(Column.of("array", ResolvedType.array(ResolvedType.array(Type.Int))))
        // ValueVector innerArr = vv(Type.Int, 1, 2, 3);
        // ValueVector midArr = vv(ResolvedType.array(Type.Int), innerArr);
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(ResolvedType.array(ResolvedType.array(Type.Int)), midArr))));
        // { 80, 76, 66, 2, 8, 8, 1, 1, 0, 1, 1, 0, 0, 0, 15, 1, 0, 1, 1, 0, 0, 0, 23, 3, 0, 1, 1, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 1 }
        ValueVector innerArr = vv(Type.Int, 1, 2, 3);
        ValueVector midArr = vv(ResolvedType.array(Type.Int), innerArr);
        assertVectorsEquals(vv(ResolvedType.array(ResolvedType.array(Type.Int)), midArr),
                PayloadReader.read(new byte[] { 80, 76, 66, 2, 8, 8, 1, 1, 0, 1, 1, 0, 0, 0, 15, 1, 0, 1, 1, 0, 0, 0, 23, 3, 0, 1, 1, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 1 }));
    }

    @Test
    void test_tablevector_1()
    {
        // Schema.of(Column.of("table", ResolvedType.table(Schema.of(Column.of("int_0", Type.Int)))))
        // Schema innerSchema = Schema.of(Column.of("int_0", Type.Int));
        // TupleVector row0 = TupleVector.of(innerSchema, List.of(vv(Type.Int, 10, 20)));
        // TupleVector row2 = TupleVector.of(innerSchema, List.of(vv(Type.Int, 30)));
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(ResolvedType.table(innerSchema), row0, row2))));
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(ResolvedType.table(innerSchema), null, null))));
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(ResolvedType.table(innerSchema), row0, null, row2))));
        // { 80, 76, 66, 2, 9, 1, 1, 2, 0, 1, 1, 1, 0, 0, 0, 20, 0, 0, 0, 37, 2, 0, 0, 0, 25, 2, 0, 1, 1, 0, 0, 0, 10, 0, 0, 0, 20, 1, 0, 0, 0, 42, 1, 0, 1, 0, 0, 0, 0, 30, 2 }
        // { 80, 76, 66, 2, 9, 1, 1, 2, 2, 2 }
        // { 80, 76, 66, 2, 9, 1, 1, 3, 1, 2, 1, 1, 1, 0, 0, 0, 25, 0, 0, 0, 0, 0, 0, 0, 42, 2, 0, 0, 0, 30, 2, 0, 1, 1, 0, 0, 0, 10, 0, 0, 0, 20, 1, 0, 0, 0, 47, 1, 0, 1, 0, 0, 0, 0, 30, 3 }
        Schema innerSchema = Schema.of(Column.of("int_0", Type.Int));
        TupleVector row0 = TupleVector.of(innerSchema, List.of(vv(Type.Int, 10, 20)));
        TupleVector row2 = TupleVector.of(innerSchema, List.of(vv(Type.Int, 30)));

        assertVectorsEquals(vv(ResolvedType.table(innerSchema), row0, row2), PayloadReader.read(
                new byte[] { 80, 76, 66, 2, 9, 1, 1, 2, 0, 1, 1, 1, 0, 0, 0, 20, 0, 0, 0, 37, 2, 0, 0, 0, 25, 2, 0, 1, 1, 0, 0, 0, 10, 0, 0, 0, 20, 1, 0, 0, 0, 42, 1, 0, 1, 0, 0, 0, 0, 30, 2 }));
        assertVectorsEquals(vv(ResolvedType.table(innerSchema), null, null), PayloadReader.read(new byte[] { 80, 76, 66, 2, 9, 1, 1, 2, 2, 2 }));
        assertVectorsEquals(vv(ResolvedType.table(innerSchema), row0, null, row2), PayloadReader.read(new byte[] {
                80, 76, 66, 2, 9, 1, 1, 3, 1, 2, 1, 1, 1, 0, 0, 0, 25, 0, 0, 0, 0, 0, 0, 0, 42, 2, 0, 0, 0, 30, 2, 0, 1, 1, 0, 0, 0, 10, 0, 0, 0, 20, 1, 0, 0, 0, 47, 1, 0, 1, 0, 0, 0, 0, 30, 3 }));
    }

    @Test
    void test_arraytablevector_1()
    {
        // Array of table - Schema.of(Column.of("array", ResolvedType.array(ResolvedType.table(Schema.of(Column.of("int_0", Type.Int), Column.of("string_1", Type.String))))))
        // Schema innerSchema = Schema.of(Column.of("int_0", Type.Int), Column.of("string_1", Type.String));
        // TupleVector row0 = TupleVector.of(innerSchema, List.of(vv(Type.Int, 10, 20), vv(Type.String, "a", "b")));
        // TupleVector row2 = TupleVector.of(innerSchema, List.of(vv(Type.Int, 30), vv(Type.String, "c")));
        // ValueVector tableVec0 = vv(ResolvedType.table(innerSchema), row0);
        // ValueVector tableVec1 = vv(ResolvedType.table(innerSchema), row2);
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(ResolvedType.array(ResolvedType.table(innerSchema)), tableVec0, tableVec1))));
        // { 80, 76, 66, 2, 8, 9, 2, 1, 5, 2, 0, 1, 1, 0, 0, 0, 21, 0, 0, 0, 67, 1, 0, 1, 1, 2, 0, 0, 0, 30, 2, 0, 0, 0, 39, 0, 0, 0, 51, 2, 0, 1, 1, 0, 0, 0, 10, 0, 0, 0, 20, 2, 0, 1, 1, 0, 0, 0, 63,
        // 0, 0, 0, 65, 1, 97, 1, 98, 1, 0, 1, 1, 2, 0, 0, 0, 76, 1, 0, 0, 0, 85, 0, 0, 0, 93, 1, 0, 1, 0, 0, 0, 0, 30, 1, 0, 1, 0, 0, 0, 0, 101, 1, 99, 1 }
        Schema innerSchema = Schema.of(Column.of("int_0", Type.Int), Column.of("string_1", Type.String));
        TupleVector row0 = TupleVector.of(innerSchema, List.of(vv(Type.Int, 10, 20), vv(Type.String, "a", "b")));
        TupleVector row2 = TupleVector.of(innerSchema, List.of(vv(Type.Int, 30), vv(Type.String, "c")));
        ValueVector tableVec0 = vv(ResolvedType.table(innerSchema), row0);
        ValueVector tableVec1 = vv(ResolvedType.table(innerSchema), row2);

        assertVectorsEquals(vv(ResolvedType.array(ResolvedType.table(innerSchema)), tableVec0, tableVec1),
                PayloadReader.read(new byte[] {
                        80, 76, 66, 2, 8, 9, 2, 1, 5, 2, 0, 1, 1, 0, 0, 0, 21, 0, 0, 0, 67, 1, 0, 1, 1, 2, 0, 0, 0, 30, 2, 0, 0, 0, 39, 0, 0, 0, 51, 2, 0, 1, 1, 0, 0, 0, 10, 0, 0, 0, 20, 2, 0, 1, 1,
                        0, 0, 0, 63, 0, 0, 0, 65, 1, 97, 1, 98, 1, 0, 1, 1, 2, 0, 0, 0, 76, 1, 0, 0, 0, 85, 0, 0, 0, 93, 1, 0, 1, 0, 0, 0, 0, 30, 1, 0, 1, 0, 0, 0, 0, 101, 1, 99, 1 }));
    }

    @Test
    void test_objectvector_1()
    {
        // Schema.of(Column.of("object", ResolvedType.object(Schema.of(Column.of("int_0", Type.Int)))))
        // Schema innerSchema = Schema.of(Column.of("int_0", Type.Int));
        // ObjectVector obj0 = ObjectVector.wrap(TupleVector.of(innerSchema, List.of(vv(Type.Int, 10))));
        // ObjectVector obj2 = ObjectVector.wrap(TupleVector.of(innerSchema, List.of(vv(Type.Int, 30))));
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(ResolvedType.object(innerSchema), obj0, obj2))));
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(ResolvedType.object(innerSchema), null, null))));
        // System.out.println(ArrayUtils.toString(PayloadWriter.write(VectorTestUtils.vv(ResolvedType.object(innerSchema), obj0, null, obj2))));
        // { 80, 76, 66, 2, 10, 1, 1, 2, 0, 1, 1, 1, 2, 0, 0, 0, 17, 2, 0, 1, 1, 0, 0, 0, 10, 0, 0, 0, 30, 2 }
        // { 80, 76, 66, 2, 10, 1, 1, 2, 2, 2 }
        // { 80, 76, 66, 2, 10, 1, 1, 3, 1, 2, 1, 1, 1, 3, 0, 0, 0, 18, 3, 1, 2, 1, 1, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0, 30, 3 }
        // NOTE! The "some null" case here is a regression pin for a real bug found and fixed in ObjectVectorWriter
        // (writing a null object row used to throw NullPointerException instead of encoding it as null) - see
        // ObjectVectorWriterTest#test_write_object_vector_with_a_null_row_not_last
        Schema innerSchema = Schema.of(Column.of("int_0", Type.Int));
        ObjectVector obj0 = ObjectVector.wrap(TupleVector.of(innerSchema, List.of(vv(Type.Int, 10))));
        ObjectVector obj2 = ObjectVector.wrap(TupleVector.of(innerSchema, List.of(vv(Type.Int, 30))));

        assertVectorsEquals(vv(ResolvedType.object(innerSchema), obj0, obj2),
                PayloadReader.read(new byte[] { 80, 76, 66, 2, 10, 1, 1, 2, 0, 1, 1, 1, 2, 0, 0, 0, 17, 2, 0, 1, 1, 0, 0, 0, 10, 0, 0, 0, 30, 2 }));
        assertVectorsEquals(vv(ResolvedType.object(innerSchema), null, null), PayloadReader.read(new byte[] { 80, 76, 66, 2, 10, 1, 1, 2, 2, 2 }));
        assertVectorsEquals(vv(ResolvedType.object(innerSchema), obj0, null, obj2),
                PayloadReader.read(new byte[] { 80, 76, 66, 2, 10, 1, 1, 3, 1, 2, 1, 1, 1, 3, 0, 0, 0, 18, 3, 1, 2, 1, 1, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0, 30, 3 }));
    }
}
