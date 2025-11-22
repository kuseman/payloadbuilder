package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IComparisonExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.expression.AsteriskExpression;
import se.kuseman.payloadbuilder.core.expression.ColumnExpression;
import se.kuseman.payloadbuilder.core.expression.ComparisonExpression;
import se.kuseman.payloadbuilder.core.parser.Location;

/** Test of {@link Projection} */
class ProjectionTest extends APhysicalPlanTest
{
    @Test
    void test_illegal_schema_expression_sizes()
    {
        IExpression col1 = ce("col1");
        assertThrows(IllegalArgumentException.class, () -> new Projection(0, scan(schemaDS(() ->
        {
        }, new TupleVector[0]), table, Schema.EMPTY), Schema.EMPTY, List.of(col1), table));
    }

    @Test
    void test_schema_full()
    {
        Schema schema = schema("col1", "col3");

        TupleVector tv = TupleVector.of(schema, asList(vv(ResolvedType.of(Type.Any), 3, 4), vv(ResolvedType.of(Type.Any), 5, 3)));

        IExpression col1 = ce("col1");
        IExpression col3 = ce("col3");

        MutableBoolean closed = new MutableBoolean(false);
        IPhysicalPlan plan = new Projection(1, scan(schemaDS(() -> closed.setTrue(), tv), table, Schema.EMPTY),
                Schema.of(col("col1", Type.Any), col(ResolvedType.of(Type.Boolean), "col1 >= col3"), col("col3", Type.Any)),
                asList(col1, new ComparisonExpression(IComparisonExpression.Type.GREATER_THAN_EQUAL, col1, col3), col3), null);

        //@formatter:off
        Schema expectedSchema = Schema.of(
                col("col1", ResolvedType.of(Type.Any)),
                col(ResolvedType.of(Type.Boolean), "col1 >= col3"),
                col("col3", ResolvedType.of(Type.Any)));
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
    void test_schema_less_with_asteirsk()
    {
        TableSourceReference tableA = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("tableA"), "a");
        TableSourceReference tableB = new TableSourceReference(1, TableSourceReference.Type.TABLE, "", QualifiedName.of("tableB"), "b");

        //@formatter:off
        Schema runtimeSchema = Schema.of(
                col("col1", Type.Any, tableA),
                col("col2", Type.Any, tableA),
                col("col3", Type.Any, tableA),
                col("col1", Type.Any, tableB),
                col("col2", Type.Any, tableB),
                col("col3", Type.Any, tableB));
        
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
        IExpression aastExp = new AsteriskExpression(QualifiedName.of("a"), null, Set.of(tableA));

        // Single column from table b
        IExpression bcol3Exp = cre("col3", tableB);

        // Computed values from both
        IExpression calcExp = gt(cre("col2", tableA), cre("col2", tableB));

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
                return schemaLessDS(() -> closed.setTrue(), tv).execute(context);
            }
        };

        IPhysicalPlan plan = new Projection(1, input, Schema.of(ast("", "a.*", tableA), col("col3", ResolvedType.of(Type.Any), tableB), col(ResolvedType.of(Type.Boolean), "a.col2 > b.col2")),
                asList(aastExp, bcol3Exp, calcExp), null);

        // Asterisks => empty schema
        //@formatter:off
        Assertions.assertThat(plan.getSchema())
            .usingRecursiveComparison()
            .ignoringFieldsOfTypes(Location.class, Random.class)
            .isEqualTo(Schema.of(
                ast("", "a.*", tableA),
                col("col3", ResolvedType.of(Type.Any), tableB),
                col(ResolvedType.of(Type.Boolean), "a.col2 > b.col2")));
        //@formatter:on

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        //@formatter:off
        Schema expectedSchema = Schema.of(
                col("col1", Type.Any, tableA),
                col("col2", Type.Any, tableA),
                col("col3", Type.Any, tableA),
                col("col3", Type.Any, tableB),
                col(ResolvedType.of(Type.Boolean), "a.col2 > b.col2"));
        //@formatter:on

        Assertions.assertThat(actual.getSchema())
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expectedSchema);
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
    void test_outer_reference_is_preserved_until_evaluation()
    {
        Schema schema = schema("col1", "col3");

        TupleVector tv = TupleVector.of(schema, asList(vv(ResolvedType.of(Type.Any), 3, 4), vv(ResolvedType.of(Type.Any), 5, 3)));

        List<ValueVector> outerVectors = new ArrayList<>();

        IExpression col1 = ce("col1");
        IExpression col3 = ce("col3");
        IExpression ocol4 = new ColumnExpression("col4", "col4", ResolvedType.of(Type.Any), null, 0, true, -1, CoreColumn.Type.REGULAR)
        {
            @Override
            public ValueVector eval(TupleVector input, IExecutionContext context)
            {
                ValueVector result = super.eval(input, context);
                outerVectors.add(result);
                return result;
            }
        };

        // formatter:off
        MutableBoolean closed = new MutableBoolean(false);
        IPhysicalPlan plan = new Projection(1, scan(schemaDS(() -> closed.setTrue(), tv), table, Schema.EMPTY),
                Schema.of(Column.of("col1", Type.Any), Column.of("col3", Type.Any), Column.of("col4", Type.Any)), asList(col1, col3, ocol4), null);
        // formatter:on

        Schema outerSchema = schema("col4");

        for (int i = 0; i < 3; i++)
        {
            context.getStatementContext()
                    .setOuterTupleVector(TupleVector.of(outerSchema, asList(vv(Type.Any, i, i))));
            TupleIterator iterator = plan.execute(context);
            TupleVector actual = PlanUtils.concat(context, iterator);

            // Traverse vectors to trigg evaluation
            for (int c = 0; c < actual.getSchema()
                    .getSize(); c++)
            {
                for (int r = 0; r < actual.getRowCount(); r++)
                {
                    actual.getColumn(c)
                            .getAny(r);
                }
            }
        }

        assertVectorsEquals(outerVectors.get(0), vv(ResolvedType.of(Type.Any), 0, 0));
        assertVectorsEquals(outerVectors.get(1), vv(ResolvedType.of(Type.Any), 1, 1));
        assertVectorsEquals(outerVectors.get(2), vv(ResolvedType.of(Type.Any), 2, 2));
    }
}
