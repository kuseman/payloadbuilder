package se.kuseman.payloadbuilder.core.expression;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.entry;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.ofEntries;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.ObjectVector;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.parser.ParseException;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;

/** Test of {@link DereferenceExpression} */
public class DereferenceExpressionTest extends APhysicalPlanTest
{
    @Test
    public void test_outerreference()
    {
        IExpression e = DereferenceExpression.create(ce("a"), QualifiedName.of("b"), null);
        assertFalse(e.isOuterReference());

        e = DereferenceExpression.create(oce("a"), QualifiedName.of("b"), null);
        assertTrue(e.isOuterReference());

        e = DereferenceExpression.create(oce("a"), QualifiedName.of("b", "c"), null);
        assertTrue(e.isOuterReference());
    }

    @Test
    public void test_dereference_map()
    {
        TupleVector tv;
        IExpression e;
        ValueVector actual;

        e = DereferenceExpression.create(ce("a"), QualifiedName.of("b"), null);

        tv = TupleVector.of(Schema.of(col("a", Type.Any)), asList(vv(ResolvedType.of(Type.Any), ofEntries(entry("b", 123)), null, ofEntries(entry("b", 456)))));

        assertEquals(QualifiedName.of("a", "b"), e.getQualifiedColumn());
        assertEquals(ResolvedType.of(Type.Any), e.getType());
        actual = e.eval(tv, context);
        assertEquals(ResolvedType.of(Type.Any), actual.type());
        assertEquals(3, actual.size());

        assertEquals(123, actual.getAny(0));
        assertTrue(actual.isNull(1));
        assertEquals(456, actual.getAny(2));
    }

    @Test
    public void test_dereference_objectvector_missing_column()
    {
        Schema innerSchema = Schema.of(col("b", Type.Int));

        try
        {
            DereferenceExpression.create(ce("a", ResolvedType.object(innerSchema)), QualifiedName.of("c"), null);
            fail("Should fail cause of missing object column");
        }
        catch (ParseException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("No column found in object named: c, expected one of: [b (Int)]"));
        }
    }

    @Test
    public void test_dereference_objectvector_by_ordinal()
    {
        TupleVector tv;
        IExpression e;
        ValueVector actual;

        Schema innerSchema = Schema.of(col("b", Type.Int));
        Schema schema = Schema.of(new Column("a", ResolvedType.object(innerSchema)));

        e = DereferenceExpression.create(ce("a", ResolvedType.object(innerSchema)), QualifiedName.of("b"), null);
        //@formatter:off
        tv = TupleVector.of(schema, asList(
                vv(ResolvedType.object(innerSchema),
                        ObjectVector.wrap(TupleVector.of(innerSchema, asList(vv(ResolvedType.of(Type.Int), 4, 5)))),
                        ObjectVector.wrap(TupleVector.of(innerSchema, asList(vv(ResolvedType.of(Type.Int), 6, 7)))))));
        //@formatter:on
        assertEquals(ResolvedType.of(Type.Int), e.getType());
        actual = e.eval(tv, context);
        assertEquals(ResolvedType.of(Type.Int), actual.type());
        assertEquals(2, actual.size());

        assertVectorsEquals(vv(ResolvedType.of(Type.Int), 4, 6), actual);
    }

    @Test
    public void test_dereference_objectvector_by_name()
    {
        TupleVector tv;
        IExpression e;
        ValueVector actual;

        Schema innerSchema = Schema.of(col("b", Type.Int));
        Schema schema = Schema.of(new Column("a", ResolvedType.object(innerSchema)));

        e = new DereferenceExpression(ce("a", ResolvedType.object(innerSchema)), "b", -1, ResolvedType.of(Type.Any));
        //@formatter:off
        tv = TupleVector.of(schema, asList(
                vv(ResolvedType.object(innerSchema),
                        ObjectVector.wrap(TupleVector.of(innerSchema, asList(vv(ResolvedType.of(Type.Int), 4, 5)))),
                        ObjectVector.wrap(TupleVector.of(innerSchema, asList(vv(ResolvedType.of(Type.Int), 6, 7)))))));
        //@formatter:on
        assertEquals(ResolvedType.of(Type.Any), e.getType());
        actual = e.eval(tv, context);
        assertEquals(ResolvedType.of(Type.Any), actual.type());
        assertEquals(2, actual.size());

        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 4, 6), actual);
    }

    @Test
    public void test_dereference_tuplevector_by_ordinal()
    {
        TupleVector tv;
        IExpression e;
        ValueVector actual;

        Schema innerSchema = Schema.of(col("b", Type.Int));
        Schema schema = Schema.of(new Column("a", ResolvedType.table(innerSchema)));

        // Test nested tuple vector, this will return a vector of value vectors
        e = DereferenceExpression.create(ce("a", ResolvedType.table(innerSchema)), QualifiedName.of("b"), null);
        //@formatter:off
        tv = TupleVector.of(schema, asList(
                vv(ResolvedType.table(innerSchema),
                        TupleVector.of(innerSchema, asList(vv(ResolvedType.of(Type.Int), 4, 5))),
                        TupleVector.of(innerSchema, asList(vv(ResolvedType.of(Type.Int), 6, 7))))));
        //@formatter:on
        assertEquals(ResolvedType.array(ResolvedType.of(Type.Int)), e.getType());
        actual = e.eval(tv, context);
        assertEquals(ResolvedType.array(ResolvedType.of(Type.Int)), actual.type());
        assertEquals(2, actual.size());

        assertVectorsEquals(vv(ResolvedType.of(Type.Int), 4, 5), actual.getArray(0));
        assertVectorsEquals(vv(ResolvedType.of(Type.Int), 6, 7), actual.getArray(1));
    }

    @Test
    public void test_dereference_tuplevector_by_name()
    {
        TupleVector tv;
        IExpression e;
        ValueVector actual;

        TableSourceReference tableA = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("table"), "");

        Schema runtimeInnerSchema = Schema.of(col("b", Type.Int));
        Schema planInnerSchema = Schema.of(new CoreColumn("b", ResolvedType.of(Type.Int), "", false, tableA, CoreColumn.Type.ASTERISK));
        Schema schema = Schema.of(new Column("a", ResolvedType.table(runtimeInnerSchema)));

        // Test nested tuple vector, this will return a vector of value vectors
        e = DereferenceExpression.create(ce("a", ResolvedType.table(planInnerSchema)), QualifiedName.of("b"), null);
        //@formatter:off
        tv = TupleVector.of(schema, asList(
                vv(ResolvedType.table(runtimeInnerSchema),
                        TupleVector.of(runtimeInnerSchema, asList(vv(ResolvedType.of(Type.Int), 4, 5))),
                        TupleVector.of(runtimeInnerSchema, asList(vv(ResolvedType.of(Type.Int), 6, 7))))));
        //@formatter:on
        assertEquals(ResolvedType.array(ResolvedType.of(Type.Int)), e.getType());
        actual = e.eval(tv, context);
        assertEquals(ResolvedType.array(ResolvedType.of(Type.Int)), actual.type());
        assertEquals(2, actual.size());

        assertVectorsEquals(vv(ResolvedType.of(Type.Int), 4, 5), actual.getArray(0));
        assertVectorsEquals(vv(ResolvedType.of(Type.Int), 6, 7), actual.getArray(1));
    }

    @Test
    public void test_dereference_tuplevector_by_name_with_no_match()
    {
        TupleVector tv;
        IExpression e;
        ValueVector actual;

        TableSourceReference tableA = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("table"), "");

        Schema runtimeInnerSchema = Schema.of(col("b", Type.Int));
        Schema planInnerSchema = Schema.of(new CoreColumn("b", ResolvedType.of(Type.Int), "", false, tableA, CoreColumn.Type.ASTERISK));
        Schema schema = Schema.of(new Column("a", ResolvedType.table(runtimeInnerSchema)));

        // Test nested tuple vector, this will return a vector of value vectors
        e = DereferenceExpression.create(ce("a", ResolvedType.table(planInnerSchema)), QualifiedName.of("c"), null);
        //@formatter:off
        tv = TupleVector.of(schema, asList(
                vv(ResolvedType.table(runtimeInnerSchema),
                        TupleVector.of(runtimeInnerSchema, asList(vv(ResolvedType.of(Type.Int), 4, 5))),
                        TupleVector.of(runtimeInnerSchema, asList(vv(ResolvedType.of(Type.Int), 6, 7))))));
        //@formatter:on
        assertEquals(ResolvedType.array(ResolvedType.of(Type.Any)), e.getType());
        actual = e.eval(tv, context);
        assertEquals(ResolvedType.array(ResolvedType.of(Type.Any)), actual.type());
        assertEquals(2, actual.size());

        assertVectorsEquals(vv(ResolvedType.of(Type.Any)), actual.getArray(0));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any)), actual.getArray(1));
    }
}
