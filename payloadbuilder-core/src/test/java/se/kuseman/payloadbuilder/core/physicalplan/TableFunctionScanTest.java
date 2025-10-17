package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

import java.util.List;
import java.util.NoSuchElementException;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.QueryException;
import se.kuseman.payloadbuilder.core.catalog.system.SystemCatalog;

/** Test {@link TableFunctionScan} */
public class TableFunctionScanTest extends APhysicalPlanTest
{
    @Test
    public void test_no_such_element()
    {
        Schema schema = Schema.of(col("Value", ResolvedType.of(Type.Int), table));
        IPhysicalPlan plan = new TableFunctionScan(0, schema, table, "", "System", SystemCatalog.get()
                .getTableFunction("range"), asList(intLit(1), intLit(10)), emptyList());

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
    public void test_that_a_table_source_is_attched_on_schema()
    {
        Schema schema = Schema.of(col("Value", ResolvedType.of(Type.Int), table));
        IPhysicalPlan plan = new TableFunctionScan(0, schema, table, "", "System", SystemCatalog.get()
                .getTableFunction("range"), asList(intLit(1), intLit(10)), emptyList());

        TupleVector vector = PlanUtils.concat(context, plan.execute(context));

        assertEquals(schema, vector.getSchema());
    }

    @Test
    public void test_validation_asterisk_with_empty_runtime_schema()
    {
        Schema schema = Schema.of(ast("col", table));
        IPhysicalPlan plan = new TableFunctionScan(0, schema, table, "", "System", new TableFunctionInfo("dummy")
        {

            @Override
            public TupleIterator execute(IExecutionContext context, String catalogAlias, List<IExpression> arguments, FunctionData data)
            {
                return TupleIterator.singleton(TupleVector.CONSTANT);
            }
        }, emptyList(), emptyList());
        try
        {
            plan.execute(context)
                    .next();
            fail("Should fail");
        }
        catch (QueryException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("returned an empty schema"));
        }
    }

    @Test
    public void test_validation_not_matching_runtime_schema_by_size()
    {
        Schema schema = Schema.of(col("Value", ResolvedType.of(Type.Int), table), col("Value2", ResolvedType.of(Type.Int), table));
        IPhysicalPlan plan = new TableFunctionScan(0, schema, table, "", "System", new TableFunctionInfo("dummy")
        {

            @Override
            public TupleIterator execute(IExecutionContext context, String catalogAlias, List<IExpression> arguments, FunctionData data)
            {
                return TupleIterator.singleton(TupleVector.of(Schema.of(Column.of("Value1", ResolvedType.INT)), ValueVector.literalInt(1, 1)));
            }
        }, emptyList(), emptyList());
        try
        {
            plan.execute(context)
                    .next();
            fail("Should fail");
        }
        catch (QueryException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("doesn't match the planned schema"));
        }
    }

    @Test
    public void test_validation_not_matching_runtime_schema_by_column()
    {
        Schema schema = Schema.of(col("Value", ResolvedType.of(Type.Int), table));
        IPhysicalPlan plan = new TableFunctionScan(0, schema, table, "", "System", new TableFunctionInfo("dummy")
        {

            @Override
            public TupleIterator execute(IExecutionContext context, String catalogAlias, List<IExpression> arguments, FunctionData data)
            {
                return TupleIterator.singleton(TupleVector.of(Schema.of(Column.of("Value1", ResolvedType.INT)), ValueVector.literalInt(1, 1)));
            }
        }, emptyList(), emptyList());
        try
        {
            plan.execute(context)
                    .next();
            fail("Should fail");
        }
        catch (QueryException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("doesn't match the planned schema"));
        }
    }

    @Test
    public void test_validation_not_matching_runtime_schema_by_type()
    {
        Schema schema = Schema.of(col("Value", ResolvedType.of(Type.Int), table));
        IPhysicalPlan plan = new TableFunctionScan(0, schema, table, "", "System", new TableFunctionInfo("dummy")
        {
            @Override
            public TupleIterator execute(IExecutionContext context, String catalogAlias, List<IExpression> arguments, FunctionData data)
            {
                return TupleIterator.singleton(TupleVector.of(Schema.of(Column.of("Value", ResolvedType.FLOAT)), ValueVector.literalFloat(1, 1)));
            }
        }, emptyList(), emptyList());
        try
        {
            plan.execute(context)
                    .next();
            fail("Should fail");
        }
        catch (QueryException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("doesn't match the planned schema"));
        }
    }
}
