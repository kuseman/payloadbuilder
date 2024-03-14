package se.kuseman.payloadbuilder.core.execution;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.nio.charset.StandardCharsets;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.api.execution.EpochDateTime;
import se.kuseman.payloadbuilder.api.execution.EpochDateTimeOffset;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Test of {@link VectorUtils} */
public class VectorUtilsTest extends APhysicalPlanTest
{
    @Test
    public void test_populate_cartesian()
    {
        Schema innerSchema = Schema.of(Column.of("col3", Type.Boolean), Column.of("col4", Type.String));
        //@formatter:off
        TupleVector vector1 = TupleVector.of(Schema.of(Column.of("col1", Type.Int), Column.of("col2", Type.Float)), asList(
                vv(Type.Int, 1, 2, 3, 4),
                vv(Type.Float, 4F, 5F, 6F, 7F)
                ));

        TupleVector vector2 = TupleVector.of(innerSchema, asList(
                vv(Type.Boolean, true, false, null),
                vv(Type.String, "hello", null, "world")
                ));
        TupleVector expected = TupleVector.of(Schema.of(Column.of("col1", Type.Int), Column.of("col2", Type.Float), pop("p", ResolvedType.table(innerSchema), null)), asList(
                vv(Type.Int, 1, 2, 3, 4),
                vv(Type.Float, 4F, 5F, 6F, 7F),
                vv(ResolvedType.table(innerSchema), vector2, vector2, vector2, vector2)
                ));
        //@formatter:on
        TupleVector actual = VectorUtils.populateCartesian(vector1, vector2, "p");
        VectorTestUtils.assertTupleVectorsEquals(expected, actual);
    }

    @Test
    public void test_cartesian()
    {
        //@formatter:off
        TupleVector vector1 = TupleVector.of(Schema.of(Column.of("col1", Type.Int), Column.of("col2", Type.Float)), asList(
                vv(Type.Int, 1, 2, 3, 4),
                vv(Type.Float, 4F, 5F, 6F, 7F)
                ));

        TupleVector vector2 = TupleVector.of(Schema.of(Column.of("col3", Type.Boolean), Column.of("col4", Type.String)), asList(
                vv(Type.Boolean, true, false, null),
                vv(Type.String, "hello", null, "world")
                ));
        TupleVector expected = TupleVector.of(Schema.of(Column.of("col1", Type.Int), Column.of("col2", Type.Float), Column.of("col3", Type.Boolean), Column.of("col4", Type.String)), asList(
                vv(Type.Int, 1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4),
                vv(Type.Float, 4F, 4F, 4F, 5F, 5F, 5F, 6F, 6F, 6F, 7F, 7F, 7F),
                vv(Type.Boolean, true, false, null, true, false, null, true, false, null, true, false, null),
                vv(Type.String, "hello", null, "world", "hello", null, "world", "hello", null, "world", "hello", null, "world")
                ));
        //@formatter:on
        TupleVector actual = VectorUtils.cartesian(vector1, vector2);
        VectorTestUtils.assertTupleVectorsEquals(expected, actual);
    }

    @Test
    public void test_hash()
    {
        assertEquals(629, VectorUtils.hash(new ValueVector[] { vv(ResolvedType.of(Type.Int), new Object[] { null }) }, 0));
        assertEquals(630, VectorUtils.hash(new ValueVector[] { vv(Type.Int, 1) }, 0));
        assertEquals(630, VectorUtils.hash(new ValueVector[] { vv(Type.Long, 1L) }, 0));
        assertEquals(31000635, VectorUtils.hash(new ValueVector[] { vv(Type.Decimal, Decimal.from(1L)) }, 0));
        assertEquals(1065353845, VectorUtils.hash(new ValueVector[] { vv(Type.Float, 1.0F) }, 0));
        assertEquals(1072693877, VectorUtils.hash(new ValueVector[] { vv(Type.Double, 1.0D) }, 0));
        assertEquals(629, VectorUtils.hash(new ValueVector[] { vv(Type.Boolean, true) }, 0));
        assertEquals(630, VectorUtils.hash(new ValueVector[] { vv(Type.Boolean, false) }, 0));
        assertEquals(1379028554, VectorUtils.hash(new ValueVector[] { vv(Type.String, "hello") }, 0));
        assertEquals(1379028554, VectorUtils.hash(new ValueVector[] { vv(Type.String, UTF8String.utf8("hello".getBytes(StandardCharsets.UTF_8))) }, 0));
        assertEquals(99162951, VectorUtils.hash(new ValueVector[] { vv(Type.Any, "hello") }, 0));
        assertEquals(1086210677, VectorUtils.hash(new ValueVector[] { vv(Type.DateTime, EpochDateTime.from(160_000_000_000L)) }, 0));
        assertEquals(1086210677, VectorUtils.hash(new ValueVector[] { vv(Type.DateTimeOffset, EpochDateTimeOffset.from(160_000_000_000L)) }, 0));
    }

    @Test
    public void test_equals()
    {
        assertEquals(false, VectorUtils.equals(new ValueVector[] { VectorTestUtils.vv(ResolvedType.of(Type.Int), new Object[] { null, 10 }) }, 0, 1));
        assertEquals(true, VectorUtils.equals(new ValueVector[] { vv(ResolvedType.of(Type.Int), new Object[] { null }) }, 0, 0));
        assertEquals(true, VectorUtils.equals(new ValueVector[] { vv(Type.Int, 1) }, 0, 0));
        assertEquals(false, VectorUtils.equals(new ValueVector[] { vv(Type.Int, 1, 2) }, 0, 1));
        assertEquals(true, VectorUtils.equals(new ValueVector[] { vv(Type.Long, 1L) }, 0, 0));
        assertEquals(false, VectorUtils.equals(new ValueVector[] { vv(Type.Long, 1L, 2L) }, 0, 1));
        assertEquals(true, VectorUtils.equals(new ValueVector[] { vv(Type.Decimal, Decimal.from(1L)) }, 0, 0));
        assertEquals(false, VectorUtils.equals(new ValueVector[] { vv(Type.Decimal, Decimal.from(1L), Decimal.from(2L)) }, 0, 1));
        assertEquals(true, VectorUtils.equals(new ValueVector[] { vv(Type.Float, 1.0F) }, 0, 0));
        assertEquals(false, VectorUtils.equals(new ValueVector[] { vv(Type.Float, 1.0F, 2.0F) }, 0, 1));
        assertEquals(true, VectorUtils.equals(new ValueVector[] { vv(Type.Double, 1.0D) }, 0, 0));
        assertEquals(false, VectorUtils.equals(new ValueVector[] { vv(Type.Double, 1.0D, 2.0D) }, 0, 1));
        assertEquals(true, VectorUtils.equals(new ValueVector[] { vv(Type.Boolean, true) }, 0, 0));
        assertEquals(false, VectorUtils.equals(new ValueVector[] { vv(Type.Boolean, true, false) }, 0, 1));
        assertEquals(true, VectorUtils.equals(new ValueVector[] { vv(Type.String, "hello") }, 0, 0));
        assertEquals(false, VectorUtils.equals(new ValueVector[] { vv(Type.String, "hello", "world") }, 0, 1));
        assertEquals(true, VectorUtils.equals(new ValueVector[] { vv(Type.Any, "hello") }, 0, 0));
        assertEquals(false, VectorUtils.equals(new ValueVector[] { vv(Type.Any, "hello", "world") }, 0, 1));
        assertEquals(true, VectorUtils.equals(new ValueVector[] { vv(Type.DateTime, EpochDateTime.from(160_000_000_000L)) }, 0, 0));
        assertEquals(false, VectorUtils.equals(new ValueVector[] { vv(Type.DateTime, EpochDateTime.from(160_000_000_000L), EpochDateTime.from(170_000_000_000L)) }, 0, 1));
        assertEquals(true, VectorUtils.equals(new ValueVector[] { vv(Type.DateTimeOffset, EpochDateTimeOffset.from(160_000_000_000L)) }, 0, 0));
        assertEquals(false, VectorUtils.equals(new ValueVector[] { vv(Type.DateTimeOffset, EpochDateTimeOffset.from(160_000_000_000L), EpochDateTimeOffset.from(170_000_000_000L)) }, 0, 1));
    }

    @Test
    public void test_compare()
    {
        assertEquals(0, VectorUtils.compare(vv(Type.Int, 1), vv(Type.Int, 1), Type.Int, 0, 0));
        assertEquals(-1, VectorUtils.compare(vv(Type.Int, 1), vv(Type.Int, 2), Type.Int, 0, 0));
        assertEquals(1, VectorUtils.compare(vv(Type.Int, 2), vv(Type.Int, 1), Type.Int, 0, 0));

        assertEquals(0, VectorUtils.compare(vv(Type.Int, 1), vv(Type.Int, 1), Type.Long, 0, 0));
        assertEquals(-1, VectorUtils.compare(vv(Type.Int, 1), vv(Type.Int, 2), Type.Long, 0, 0));
        assertEquals(1, VectorUtils.compare(vv(Type.Int, 2), vv(Type.Int, 1), Type.Long, 0, 0));

        assertEquals(0, VectorUtils.compare(vv(Type.Int, 1), vv(Type.Int, 1), Type.Decimal, 0, 0));
        assertEquals(-1, VectorUtils.compare(vv(Type.Int, 1), vv(Type.Int, 2), Type.Decimal, 0, 0));
        assertEquals(1, VectorUtils.compare(vv(Type.Int, 2), vv(Type.Int, 1), Type.Decimal, 0, 0));

        assertEquals(0, VectorUtils.compare(vv(Type.Int, 1), vv(Type.Int, 1), Type.Float, 0, 0));
        assertEquals(-1, VectorUtils.compare(vv(Type.Int, 1), vv(Type.Int, 2), Type.Float, 0, 0));
        assertEquals(1, VectorUtils.compare(vv(Type.Int, 2), vv(Type.Int, 1), Type.Float, 0, 0));

        assertEquals(0, VectorUtils.compare(vv(Type.Int, 1), vv(Type.Int, 1), Type.Double, 0, 0));
        assertEquals(-1, VectorUtils.compare(vv(Type.Int, 1), vv(Type.Int, 2), Type.Double, 0, 0));
        assertEquals(1, VectorUtils.compare(vv(Type.Int, 2), vv(Type.Int, 1), Type.Double, 0, 0));

        assertEquals(0, VectorUtils.compare(vv(Type.Int, 1), vv(Type.Int, 1), Type.Boolean, 0, 0));
        assertEquals(-1, VectorUtils.compare(vv(Type.Int, 0), vv(Type.Int, 1), Type.Boolean, 0, 0));
        assertEquals(1, VectorUtils.compare(vv(Type.Int, 1), vv(Type.Int, 0), Type.Boolean, 0, 0));

        assertEquals(0, VectorUtils.compare(vv(Type.Int, 1), vv(Type.Int, 1), Type.String, 0, 0));
        assertEquals(-1, VectorUtils.compare(vv(Type.Int, 1), vv(Type.Int, 2), Type.String, 0, 0));
        assertEquals(1, VectorUtils.compare(vv(Type.Int, 2), vv(Type.Int, 1), Type.String, 0, 0));

        assertEquals(0, VectorUtils.compare(vv(Type.String, "2010-10-10"), vv(Type.String, "2010-10-10"), Type.DateTime, 0, 0));
        assertEquals(-1, VectorUtils.compare(vv(Type.String, "2010-09-10"), vv(Type.String, "2010-10-10"), Type.DateTime, 0, 0));
        assertEquals(1, VectorUtils.compare(vv(Type.String, "2010-10-10"), vv(Type.String, "2010-09-10"), Type.DateTime, 0, 0));

        assertEquals(0, VectorUtils.compare(vv(Type.String, "2010-10-10"), vv(Type.String, "2010-10-10"), Type.DateTimeOffset, 0, 0));
        assertEquals(-1, VectorUtils.compare(vv(Type.String, "2010-09-10"), vv(Type.String, "2010-10-10"), Type.DateTimeOffset, 0, 0));
        assertEquals(1, VectorUtils.compare(vv(Type.String, "2010-10-10"), vv(Type.String, "2010-09-10"), Type.DateTimeOffset, 0, 0));

        assertEquals(0, VectorUtils.compare(vv(Type.Int, 1), vv(Type.Int, 1), Type.Any, 0, 0));
        assertEquals(-1, VectorUtils.compare(vv(Type.Int, 1), vv(Type.Int, 2), Type.Any, 0, 0));
        assertEquals(1, VectorUtils.compare(vv(Type.Int, 2), vv(Type.Int, 1), Type.Any, 0, 0));
    }

}
