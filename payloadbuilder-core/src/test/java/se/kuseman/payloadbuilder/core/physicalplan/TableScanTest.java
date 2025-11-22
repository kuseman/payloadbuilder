package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.NoSuchElementException;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.core.QueryException;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;

/** Test {@link TableScan} */
class TableScanTest extends APhysicalPlanTest
{
    @Test
    void test_runtime_schema_is_set()
    {
        TableSourceReference table = new TableSourceReference(666, TableSourceReference.Type.TABLE, "", QualifiedName.of("table"), "t");

        Schema schema = Schema.of(col("Value", ResolvedType.of(Type.Int), table));

        IDatasource datasource = schemaLessDS(() ->
        {
        }, TupleVector.of(schema, asList(ValueVector.literalInt(100, 100))), TupleVector.of(schema, asList(ValueVector.literalInt(100, 100))));

        TableScan plan = new TableScan(0, schema, table, "", datasource, emptyList());
        TupleIterator it = plan.execute(context);
        while (it.hasNext())
        {
            it.next();
        }
        assertEquals(schema, context.getStatementContext()
                .getRuntimeSchema(666));
    }

    @Test
    void test_no_such_element()
    {
        Schema schema = Schema.of(col("Value", ResolvedType.of(Type.Int), table));
        IPhysicalPlan plan = scan(schemaLessDS(() ->
        {
        }, TupleVector.of(schema, asList(ValueVector.literalInt(100, 100))), TupleVector.of(schema, asList(ValueVector.literalInt(100, 100)))), table, Schema.EMPTY);

        TupleIterator it = plan.execute(context);
        while (it.hasNext())
        {
            it.next();
        }
        try
        {
            it.next();
            fail("No such element");
        }
        catch (NoSuchElementException e)
        {
        }
    }

    @Test
    void test_that_a_table_source_is_attched_on_schema()
    {
        MutableBoolean closed = new MutableBoolean(false);
        Schema schema = Schema.of(Column.of("col", Type.Int));
        IPhysicalPlan plan = scan(
                schemaLessDS(() -> closed.setTrue(), TupleVector.of(schema, asList(ValueVector.literalInt(100, 100))), TupleVector.of(schema, asList(ValueVector.literalInt(100, 100)))), table,
                Schema.EMPTY);

        assertEquals(Schema.EMPTY, plan.getSchema());

        TupleVector vector = PlanUtils.concat(context, plan.execute(context));

        Assertions.assertThat(vector.getSchema())
                .usingRecursiveComparison()
                .isEqualTo(Schema.of(col("col", ResolvedType.of(Type.Int), table)));

        assertEquals(Schema.of(col("col", ResolvedType.of(Type.Int), table)), vector.getSchema());
    }

    @Test
    void test_that_a_table_source_is_rejecting_asterisk_in_actual_schema()
    {
        MutableBoolean closed = new MutableBoolean(false);
        // Asterisk schema
        Schema schema = Schema.of(ast("t", table));
        IPhysicalPlan plan = scan(
                schemaLessDS(() -> closed.setTrue(), TupleVector.of(schema, asList(ValueVector.literalInt(100, 100))), TupleVector.of(schema, asList(ValueVector.literalInt(100, 100)))), table,
                schema);

        assertEquals(schema, plan.getSchema());

        try
        {
            PlanUtils.concat(context, plan.execute(context));
            fail("Should fail because of asterisks");
        }
        catch (QueryException e)
        {
            assertTrue(e.getMessage()
                    .contains("Runtime tuple vectors cannot contain asterisk columns"), e.getMessage());
        }
    }

    @Test
    void test_validation_asterisk_with_empty_runtime_schema()
    {
        Schema schema = Schema.of(ast("col", table));
        IPhysicalPlan plan = new TableScan(0, schema, table, "System", new IDatasource()
        {
            @Override
            public TupleIterator execute(IExecutionContext context)
            {
                return TupleIterator.singleton(TupleVector.CONSTANT);
            }
        }, emptyList());

        try
        {
            plan.execute(context)
                    .next();
            fail("Should fail");
        }
        catch (QueryException e)
        {
            assertTrue(e.getMessage()
                    .contains("returned an empty schema"), e.getMessage());
        }
    }

    @Test
    void test_validation_not_matching_runtime_schema_by_size()
    {
        Schema schema = Schema.of(col("Value", ResolvedType.of(Type.Int), table), col("Value2", ResolvedType.of(Type.Int), table));
        IPhysicalPlan plan = new TableScan(0, schema, table, "System", new IDatasource()
        {
            @Override
            public TupleIterator execute(IExecutionContext context)
            {
                return TupleIterator.singleton(TupleVector.of(Schema.of(Column.of("Value1", ResolvedType.INT)), ValueVector.literalInt(1, 1)));
            }
        }, emptyList());
        try
        {
            plan.execute(context)
                    .next();
            fail("Should fail");
        }
        catch (QueryException e)
        {
            assertTrue(e.getMessage()
                    .contains("doesn't match the planned schema"), e.getMessage());
        }
    }

    @Test
    void test_validation_not_matching_runtime_schema_by_column()
    {
        Schema schema = Schema.of(col("Value", ResolvedType.of(Type.Int), table));
        IPhysicalPlan plan = new TableScan(0, schema, table, "System", new IDatasource()
        {
            @Override
            public TupleIterator execute(IExecutionContext context)
            {
                return TupleIterator.singleton(TupleVector.of(Schema.of(Column.of("Value1", ResolvedType.INT)), ValueVector.literalInt(1, 1)));
            }
        }, emptyList());
        try
        {
            plan.execute(context)
                    .next();
            fail("Should fail");
        }
        catch (QueryException e)
        {
            assertTrue(e.getMessage()
                    .contains("doesn't match the planned schema"), e.getMessage());
        }
    }

    @Test
    void test_validation_not_matching_runtime_schema_by_type()
    {
        Schema schema = Schema.of(col("Value", ResolvedType.of(Type.Int), table));
        IPhysicalPlan plan = new TableScan(0, schema, table, "System", new IDatasource()
        {
            @Override
            public TupleIterator execute(IExecutionContext context)
            {
                return TupleIterator.singleton(TupleVector.of(Schema.of(Column.of("Value", ResolvedType.FLOAT)), ValueVector.literalFloat(1, 1)));
            }
        }, emptyList());

        try
        {
            plan.execute(context)
                    .next();
            fail("Should fail");
        }
        catch (QueryException e)
        {
            assertTrue(e.getMessage()
                    .contains("doesn't match the planned schema"), e.getMessage());
        }
    }
}
