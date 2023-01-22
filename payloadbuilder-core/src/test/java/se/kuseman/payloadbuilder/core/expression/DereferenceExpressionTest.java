package se.kuseman.payloadbuilder.core.expression;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.entry;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.ofEntries;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.nvv;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ColumnReference;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;

/** Test of {@link DereferenceExpression} */
public class DereferenceExpressionTest extends APhysicalPlanTest
{
    @Test
    public void test_dereference_map()
    {
        TupleVector tv;
        IExpression e;
        ValueVector actual;

        e = DereferenceExpression.create(ce("a"), QualifiedName.of("b"));

        tv = TupleVector.of(Schema.of(col("a", Type.Any)), asList(vv(ResolvedType.of(Type.Any), ofEntries(entry("b", 123)), null, ofEntries(entry("b", 456)))));

        assertEquals(QualifiedName.of("a", "b"), e.getQualifiedColumn());
        assertEquals(ResolvedType.of(Type.Any), e.getType());
        actual = e.eval(tv, context);
        assertEquals(ResolvedType.of(Type.Any), actual.type());
        assertEquals(3, actual.size());

        assertEquals(123, actual.getValue(0));
        assertTrue(actual.isNull(1));
        assertEquals(456, actual.getValue(2));
    }

    @Test
    public void test_dereference_tuplevector_by_ordinal()
    {
        TupleVector tv;
        IExpression e;
        ValueVector actual;

        Schema innerSchema = Schema.of(col("b", Type.Int));
        Schema schema = Schema.of(new Column("a", ResolvedType.tupleVector(innerSchema)));

        // Test nested tuple vector, this will return a vector of value vectors
        e = DereferenceExpression.create(ce("a", ResolvedType.tupleVector(innerSchema)), QualifiedName.of("b"));
        //@formatter:off
        tv = TupleVector.of(schema, asList(
                vv(ResolvedType.tupleVector(innerSchema),
                        TupleVector.of(innerSchema, asList(vv(ResolvedType.of(Type.Int), 4, 5))),
                        TupleVector.of(innerSchema, asList(vv(ResolvedType.of(Type.Int), 6, 7))))));
        //@formatter:on
        assertEquals(ResolvedType.valueVector(ResolvedType.of(Type.Int)), e.getType());
        actual = e.eval(tv, context);
        assertEquals(ResolvedType.valueVector(ResolvedType.of(Type.Int)), actual.type());
        assertEquals(2, actual.size());

        assertVectorsEquals(vv(ResolvedType.of(Type.Int), 4, 5), (ValueVector) actual.getValue(0));
        assertVectorsEquals(vv(ResolvedType.of(Type.Int), 6, 7), (ValueVector) actual.getValue(1));
    }

    @Test
    public void test_dereference_tuplevector_by_name()
    {
        TupleVector tv;
        IExpression e;
        ValueVector actual;

        TableSourceReference tableA = new TableSourceReference("", QualifiedName.of("table"), "");

        Schema runtimeInnerSchema = Schema.of(col("b", Type.Int));
        Schema planInnerSchema = Schema.of(new Column("b", ResolvedType.of(Type.Int), new ColumnReference(tableA, "b", ColumnReference.Type.ASTERISK)));
        Schema schema = Schema.of(new Column("a", ResolvedType.tupleVector(runtimeInnerSchema)));

        // Test nested tuple vector, this will return a vector of value vectors
        e = DereferenceExpression.create(ce("a", ResolvedType.tupleVector(planInnerSchema)), QualifiedName.of("b"));
        //@formatter:off
        tv = TupleVector.of(schema, asList(
                vv(ResolvedType.tupleVector(runtimeInnerSchema),
                        TupleVector.of(runtimeInnerSchema, asList(vv(ResolvedType.of(Type.Int), 4, 5))),
                        TupleVector.of(runtimeInnerSchema, asList(vv(ResolvedType.of(Type.Int), 6, 7))))));
        //@formatter:on
        assertEquals(ResolvedType.valueVector(ResolvedType.of(Type.Int)), e.getType());
        actual = e.eval(tv, context);
        assertEquals(ResolvedType.valueVector(ResolvedType.of(Type.Int)), actual.type());
        assertEquals(2, actual.size());

        assertVectorsEquals(vv(ResolvedType.of(Type.Int), 4, 5), (ValueVector) actual.getValue(0));
        assertVectorsEquals(vv(ResolvedType.of(Type.Int), 6, 7), (ValueVector) actual.getValue(1));
    }

    @Test
    public void test_dereference_tuplevector_by_name_with_no_match()
    {
        TupleVector tv;
        IExpression e;
        ValueVector actual;

        TableSourceReference tableA = new TableSourceReference("", QualifiedName.of("table"), "");

        Schema runtimeInnerSchema = Schema.of(col("b", Type.Int));
        Schema planInnerSchema = Schema.of(new Column("b", ResolvedType.of(Type.Int), new ColumnReference(tableA, "b", ColumnReference.Type.ASTERISK)));
        Schema schema = Schema.of(new Column("a", ResolvedType.tupleVector(runtimeInnerSchema)));

        // Test nested tuple vector, this will return a vector of value vectors
        e = DereferenceExpression.create(ce("a", ResolvedType.tupleVector(planInnerSchema)), QualifiedName.of("c"));
        //@formatter:off
        tv = TupleVector.of(schema, asList(
                vv(ResolvedType.tupleVector(runtimeInnerSchema),
                        TupleVector.of(runtimeInnerSchema, asList(vv(ResolvedType.of(Type.Int), 4, 5))),
                        TupleVector.of(runtimeInnerSchema, asList(vv(ResolvedType.of(Type.Int), 6, 7))))));
        //@formatter:on
        assertEquals(ResolvedType.valueVector(ResolvedType.of(Type.Any)), e.getType());
        actual = e.eval(tv, context);
        assertEquals(ResolvedType.valueVector(ResolvedType.of(Type.Any)), actual.type());
        assertEquals(2, actual.size());

        assertVectorsEquals(nvv(ResolvedType.of(Type.Any)), (ValueVector) actual.getValue(0));
        assertVectorsEquals(nvv(ResolvedType.of(Type.Any)), (ValueVector) actual.getValue(1));
    }
}
