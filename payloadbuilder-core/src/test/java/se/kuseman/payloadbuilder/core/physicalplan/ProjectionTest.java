package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ColumnReference;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.api.catalog.TupleIterator;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IComparisonExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.expression.ColumnExpression;
import se.kuseman.payloadbuilder.core.expression.ComparisonExpression;

/** Test of {@link Projection} */
public class ProjectionTest extends APhysicalPlanTest
{
    @Test
    public void test_schema_full()
    {
        Schema schema = schema("col1", "col3");

        TupleVector tv = TupleVector.of(schema, asList(vv(ResolvedType.of(Type.Any), 3, 4), vv(ResolvedType.of(Type.Any), 5, 3)));

        IExpression col1 = ce("col1");
        IExpression col3 = ce("col3");

        MutableBoolean closed = new MutableBoolean(false);
        IPhysicalPlan plan = new Projection(1, scan(schemaDS(() -> closed.setTrue(), tv), table, schema),
                asList(col1, new ComparisonExpression(IComparisonExpression.Type.GREATER_THAN_EQUAL, col1, col3), col3), false);

        //@formatter:off
        Schema expectedSchema = Schema.of(
                new Column("col1", "", ResolvedType.of(Type.Any), null, false),
                new Column("", "col1 >= col3", ResolvedType.of(Type.Boolean), null, false),
                new Column("col3", "", ResolvedType.of(Type.Any), null, false));
        //@formatter:on

        assertEquals(expectedSchema, plan.getSchema());

        TupleIterator it = plan.execute(context);
        // CSOFF
        TupleVector actual = it.next();
        // CSON
        assertFalse(it.hasNext());

        try
        {
            it.next();
            fail("Should fail with no such element");
        }
        catch (NoSuchElementException e)
        {
        }

        it.close();

        assertEquals(2, actual.getRowCount());
        assertEquals(expectedSchema, actual.getSchema());

        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 3, 4), actual.getColumn(0));
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), false, true), actual.getColumn(1));
        assertTrue(closed.booleanValue());

    }

    @Test
    public void test_schema_less_with_asteirsk()
    {
        TableSourceReference tableA = new TableSourceReference("", QualifiedName.of("tableA"), "a");
        TableSourceReference tableB = new TableSourceReference("", QualifiedName.of("tableB"), "b");

        // Asterisk column existing on the projection expressions from planning
        // CSOFF
        ColumnReference aAst = new ColumnReference(tableA, "a", ColumnReference.Type.ASTERISK);

        // Actual columns received runtime
        ColumnReference aCol1 = new ColumnReference(tableA, "col1", ColumnReference.Type.REGULAR);
        ColumnReference aCol2 = new ColumnReference(tableA, "col2", ColumnReference.Type.REGULAR);
        ColumnReference aCol3 = new ColumnReference(tableA, "col3", ColumnReference.Type.REGULAR);

        ColumnReference bCol1 = new ColumnReference(tableB, "col1", ColumnReference.Type.REGULAR);
        ColumnReference bCol2 = new ColumnReference(tableB, "col2", ColumnReference.Type.REGULAR);
        ColumnReference bCol3 = new ColumnReference(tableB, "col3", ColumnReference.Type.REGULAR);
        // CSON

        //@formatter:off
        Schema runtimeSchema = Schema.of(
                col(aCol1, Type.Any),
                col(aCol2, Type.Any),
                col(aCol3, Type.Any),
                col(bCol1, Type.Any),
                col(bCol2, Type.Any),
                col(bCol3, Type.Any));
        
        TupleVector tv = TupleVector.of(runtimeSchema, asList(
                // table a cols
                vv(ResolvedType.of(Type.Any), 1, 2),
                vv(ResolvedType.of(Type.Any), 3, 4),
                vv(ResolvedType.of(Type.Any), 5, 6),
                // table b cols
                vv(ResolvedType.of(Type.Any), 10, 20),
                vv(ResolvedType.of(Type.Any), 30, 40),
                vv(ResolvedType.of(Type.Any), 50, 60)
                ));
        //@formatter:on

        // All columns from table a
        IExpression aastExp = cre(aAst);

        // Single column from table b
        IExpression bcol3Exp = cre(bCol3);

        // Computed values from both
        IExpression calcExp = gt(cre(aCol2), cre(bCol2));

        MutableBoolean closed = new MutableBoolean(false);

        IPhysicalPlan input = new IPhysicalPlan()
        {
            @Override
            public Schema getSchema()
            {
                return runtimeSchema;
            }

            @Override
            public int getNodeId()
            {
                return 0;
            }

            @Override
            public List<IPhysicalPlan> getChildren()
            {
                return emptyList();
            }

            @Override
            public TupleIterator execute(IExecutionContext context)
            {
                return schemaLessDS(() -> closed.setTrue(), tv).execute(context, new DatasourceOptions(emptyList()));
            }
        };

        IPhysicalPlan plan = new Projection(1, input, asList(aastExp, bcol3Exp, calcExp), false);

        // Asterisks => empty schema
        //@formatter:off
        assertEquals(Schema.of(
                Column.of(aAst, ResolvedType.of(Type.Any)),
                Column.of(bCol3, ResolvedType.of(Type.Any)),
                new Column("", "a.col2 > b.col2", ResolvedType.of(Type.Boolean), null, false)
                ), plan.getSchema());
        //@formatter:on

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(it);

        //@formatter:off
        Schema expectedSchema = Schema.of(
                col(aCol1, Type.Any), 
                col(aCol2, Type.Any), 
                col(aCol3, Type.Any), 
                col(bCol3, Type.Any), 
                new Column("", "a.col2 > b.col2", ResolvedType.of(Type.Boolean), null, false));
        //@formatter:on

        assertEquals(expectedSchema, actual.getSchema());
        assertEquals(2, actual.getRowCount());

        // aCol1
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 1, 2), actual.getColumn(0));
        // aCol2
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 3, 4), actual.getColumn(1));
        // aCol3
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 5, 6), actual.getColumn(2));
        // bCol3
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 50, 60), actual.getColumn(3));
        // aCol2 > bCol2
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), false, false), actual.getColumn(4));

        assertTrue(closed.booleanValue());
    }

    @Test
    public void test_outer_reference_is_preserved_until_evaluation()
    {
        Schema schema = schema("col1", "col3");

        TupleVector tv = TupleVector.of(schema, asList(vv(ResolvedType.of(Type.Any), 3, 4), vv(ResolvedType.of(Type.Any), 5, 3)));

        List<ValueVector> outerVectors = new ArrayList<>();

        IExpression col1 = ce("col1");
        IExpression col3 = ce("col3");
        IExpression ocol4 = new ColumnExpression("col4", "col4", ResolvedType.of(Type.Any), null, 0, true, -1)
        {
            @Override
            public ValueVector eval(TupleVector input, IExecutionContext context)
            {
                ValueVector result = super.eval(input, context);
                outerVectors.add(result);
                return result;
            }
        };

        MutableBoolean closed = new MutableBoolean(false);
        IPhysicalPlan plan = new Projection(1, scan(schemaDS(() -> closed.setTrue(), tv), table, schema), asList(col1, col3, ocol4), false);

        Schema outerSchema = schema("col4");

        for (int i = 0; i < 3; i++)
        {
            context.getStatementContext()
                    .setOuterTupleVector(TupleVector.of(outerSchema, asList(vv(Type.Any, i, i))));
            TupleIterator iterator = plan.execute(context);
            TupleVector actual = PlanUtils.concat(iterator);

            // Traverse vectors to trigg evaluation
            for (int c = 0; c < actual.getSchema()
                    .getSize(); c++)
            {
                for (int r = 0; r < actual.getRowCount(); r++)
                {
                    actual.getColumn(c)
                            .getValue(r);
                }
            }
        }

        assertVectorsEquals(outerVectors.get(0), vv(ResolvedType.of(Type.Any), 0, 0));
        assertVectorsEquals(outerVectors.get(1), vv(ResolvedType.of(Type.Any), 1, 1));
        assertVectorsEquals(outerVectors.get(2), vv(ResolvedType.of(Type.Any), 2, 2));
    }
}
