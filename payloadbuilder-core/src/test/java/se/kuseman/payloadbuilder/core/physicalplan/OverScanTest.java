package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TupleIterator;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;

/** Test of {@link OverScan} */
public class OverScanTest extends APhysicalPlanTest
{
    @Test
    public void test_with_ordinal()
    {
        Schema innerSchema = Schema.of(Column.of("col2", Type.Any), Column.of("col3", Type.Any));
        //@formatter:off
        Schema schema = Schema.of(
                Column.of("col1", Type.Int),
                Column.of("col2", Type.Int),
                new Column("col3", ResolvedType.tupleVector(innerSchema)));
        //@formatter:on

        //@formatter:off
        context.getStatementContext()
                .setOuterTupleVector(TupleVector.of(schema, asList(
                        vv(ResolvedType.of(Type.Int), 0),
                        vv(ResolvedType.of(Type.Int), 2),
                        ValueVector.literalObject(ResolvedType.tupleVector(innerSchema),
                            TupleVector.of(innerSchema, asList(
                                    vv(ResolvedType.of(Type.Any), 4, 5, 6, 7, 8),
                                    vv(ResolvedType.of(Type.Any), 6, 7, 8, 9, 10))), 1))));
        //@formatter:on

        OverScan scan = new OverScan(0, 2, "a", innerSchema);
        assertEquals(innerSchema, scan.getSchema());

        TupleIterator it = scan.execute(context);

        int count = 0;
        while (it.hasNext())
        {
            TupleVector vector = it.next();

            assertEquals(innerSchema, vector.getSchema());
            assertEquals(5, vector.getRowCount());

            assertVectorsEquals(vv(ResolvedType.of(Type.Any), 4, 5, 6, 7, 8), vector.getColumn(0));
            assertVectorsEquals(vv(ResolvedType.of(Type.Any), 6, 7, 8, 9, 10), vector.getColumn(1));

            count += vector.getRowCount();
        }
        it.close();
        assertEquals(5, count);
    }

    @Test
    public void test_with_qualified()
    {
        Schema innerSchema = Schema.of(Column.of("col2", Type.Any), Column.of("col3", Type.Any));
        //@formatter:off
        Schema schema = Schema.of(
                Column.of("col1", Type.Int),
                Column.of("col2", Type.Int),
                new Column("col3", ResolvedType.tupleVector(innerSchema)));
        //@formatter:on

        //@formatter:off
        context.getStatementContext()
                .setOuterTupleVector(TupleVector.of(schema, asList(
                        vv(ResolvedType.of(Type.Int), 0),
                        vv(ResolvedType.of(Type.Int), 2),
                        ValueVector.literalObject(ResolvedType.tupleVector(innerSchema),
                            TupleVector.of(innerSchema, asList(
                                    vv(ResolvedType.of(Type.Any), 4, 5, 6, 7, 8),
                                    vv(ResolvedType.of(Type.Any), 6, 7, 8, 9, 10))), 1))));
        //@formatter:on

        OverScan scan = new OverScan(0, -1, "col3", innerSchema);
        assertEquals(innerSchema, scan.getSchema());

        TupleIterator it = scan.execute(context);

        int count = 0;
        while (it.hasNext())
        {
            TupleVector vector = it.next();

            assertEquals(innerSchema, vector.getSchema());
            assertEquals(5, vector.getRowCount());

            assertVectorsEquals(vv(ResolvedType.of(Type.Any), 4, 5, 6, 7, 8), vector.getColumn(0));
            assertVectorsEquals(vv(ResolvedType.of(Type.Any), 6, 7, 8, 9, 10), vector.getColumn(1));

            count += vector.getRowCount();
        }
        it.close();
        assertEquals(5, count);

        // OverScan scan = new OverScan(0, -1, "a");
        //
        // Schema innerSchema = Schema.of(Column.of("col2", Type.Object), Column.of("col3", Type.Object));
        // Schema schema = Schema.of(Column.of("col1", Type.Int), Column.of("col2", Type.Int), new Column("a", ResolvedType.tupleVector(innerSchema)));
        //
        // context.getStatementContext()
        // .setOuterTupleVector(TupleVector.of(schema,
        // asList(vv(Type.Int, 0), vv(Type.Int, 2), ValueVector.literalObject(Type.TupleVector, TupleVector.of(innerSchema, asList(vv(4, 5, 6, 7, 8), vv(6, 7, 8, 9, 10))), 1))));
        //
        // assertEquals(innerSchema, scan.getSchema());
        //
        // TupleIterator it = scan.execute(context);
        //
        // int count = 0;
        // while (it.hasNext())
        // {
        // TupleVector vector = it.next();
        //
        // assertEquals(innerSchema, vector.getSchema());
        // assertEquals(5, vector.getRowCount());
        //
        // assertVectorsEquals(vv(4, 5, 6, 7, 8), vector.getColumn(0));
        // assertVectorsEquals(vv(6, 7, 8, 9, 10), vector.getColumn(1));
        //
        // count += vector.getRowCount();
        // }
        // it.close();
        // assertEquals(5, count);
    }
}
