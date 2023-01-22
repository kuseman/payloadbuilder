package se.kuseman.payloadbuilder.core.logicalplan.optimization;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ColumnReference;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableSchema;
import se.kuseman.payloadbuilder.api.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.core.expression.AliasExpression;
import se.kuseman.payloadbuilder.core.expression.SubQueryExpression;
import se.kuseman.payloadbuilder.core.logicalplan.ConstantScan;
import se.kuseman.payloadbuilder.core.logicalplan.ILogicalPlan;
import se.kuseman.payloadbuilder.core.logicalplan.Limit;
import se.kuseman.payloadbuilder.core.logicalplan.OperatorFunctionScan;
import se.kuseman.payloadbuilder.core.logicalplan.TableFunctionScan;
import se.kuseman.payloadbuilder.core.logicalplan.TableScan;
import se.kuseman.payloadbuilder.core.parser.ParseException;

/** Test of {@link SchemaResolver} */
public class SchemaResolverTest extends ALogicalPlanOptimizerTest
{
    private SchemaResolver resolver = new SchemaResolver();

    @Test
    public void test_table_source_schema()
    {
        ILogicalPlan plan = s("select * from sys#functions");
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference tableSource = new TableSourceReference("sys", QualifiedName.of("functions"), "");
        ColumnReference name = tableSource.column("name");
        ColumnReference type = tableSource.column("type");
        ColumnReference description = tableSource.column("description");

        Schema expectedSchema = Schema.of(col(name, Type.String), col(type, Type.String), col(description, Type.String));

        //@formatter:off
        ILogicalPlan expected = new TableScan(new TableSchema(expectedSchema), tableSource, emptyList(), false, emptyList(), null);
        //@formatter:on

        assertEquals(expectedSchema, actual.getSchema());
        assertEquals(expected, actual);
    }

    @Test
    public void test_table_source_schema_with_top()
    {
        ILogicalPlan plan = s("select top 10 * from sys#functions");
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference tableSource = new TableSourceReference("sys", QualifiedName.of("functions"), "");
        ColumnReference name = tableSource.column("name");
        ColumnReference type = tableSource.column("type");
        ColumnReference description = tableSource.column("description");

        Schema expectedSchema = Schema.of(col(name, Type.String), col(type, Type.String), col(description, Type.String));

        //@formatter:off
        ILogicalPlan expected = 
                new Limit(
                        new TableScan(new TableSchema(expectedSchema), tableSource, emptyList(), false, emptyList(), null),
                        e("10"));
        //@formatter:on

        assertEquals(expectedSchema, actual.getSchema());
        assertEquals(expected, actual);
    }

    @Test
    public void test_table_function_resolving()
    {
        ILogicalPlan plan = s("select * from range(1, 10) x");
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference tableSource = new TableSourceReference("", QualifiedName.of("range"), "x");
        ColumnReference value = tableSource.column("Value");

        Schema expectedSchema = Schema.of(new Column("Value", ResolvedType.of(Type.Int), value, false));

        //@formatter:off
        ILogicalPlan expected = 
                        new TableFunctionScan(tableSource, expectedSchema, asList(intLit(1), intLit(10)), emptyList(), null);
        //@formatter:on

        assertEquals(expectedSchema, actual.getSchema());
        assertEquals(expected, actual);
    }

    @Test
    public void test_function_resolving()
    {
        ILogicalPlan plan = s("select concat('hello', 123)");
        ILogicalPlan actual = optimize(context, plan);

        Schema expectedSchema = Schema.of(new Column("", "concat('hello', 123)", ResolvedType.of(Type.String), null, false));
        assertEquals(expectedSchema, actual.getSchema());
    }

    @Test
    public void test_function_resolving_aggregate()
    {
        ILogicalPlan plan = s("select sum(col2) from table group by col");
        ILogicalPlan actual = optimize(context, plan);

        Schema expectedSchema = Schema.of(new Column("", "sum(col2)", ResolvedType.of(Type.Any), null, false));
        assertEquals(expectedSchema, actual.getSchema());
    }

    @Test
    public void test_function_resolving_aggregate_nesting_of_scalar_aggregates()
    {
        ILogicalPlan plan = s("select sum(sum(col2)) from table group by col");
        ILogicalPlan actual = optimize(context, plan);

        Schema expectedSchema = Schema.of(new Column("", "sum(sum(col2))", ResolvedType.of(Type.Any), null, false));
        assertEquals(expectedSchema, actual.getSchema());
    }

    @Test
    public void test_nested_aggregate_functions()
    {
        ILogicalPlan plan = s("select object(object('col')) from table group by col");

        try
        {
            optimize(context, plan);
            fail("Should fail cause of nested aggregates");
        }
        catch (ParseException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot aggregate expressions containing aggregate functions"));
        }
    }

    @Test
    public void test_function_resolving_arity_fail()
    {
        ILogicalPlan plan = s("select sum(col2, 10) from table group by col");

        try
        {
            optimize(context, plan);
            fail("Should fail cause arity mismatch");
        }
        catch (ParseException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Function sum expected 1 argument(s) but got 2"));
        }
    }

    @Test
    public void test_sub_query_expression()
    {
        ILogicalPlan plan = s("select 12345, (select * from sys#tables for object_array) tables");
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference tableSource = new TableSourceReference("sys", QualifiedName.of("tables"), "");
        ColumnReference name = tableSource.column("name");
        ColumnReference schema = tableSource.column("schema");
        ColumnReference rows = tableSource.column("rows");

        Schema expectedSchema = Schema.of(col(name, Type.String), col(schema, Type.String), col(rows, Type.Int));

        //@formatter:off
        ILogicalPlan expected = projection(
                ConstantScan.INSTANCE,
                asList(
                        intLit(12345),
                        new AliasExpression(
                            new SubQueryExpression(
                                new OperatorFunctionScan(
                                   Schema.of(col("output", Type.OutputWritable)),
                                   new TableScan(new TableSchema(expectedSchema), tableSource, asList(), false, emptyList(), null),
                                   "",
                                   "object_array",
                                   null
                                ), null),
                            "tables"))
                );
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        assertEquals(expected, actual);
    }

    private ILogicalPlan optimize(IExecutionContext context, ILogicalPlan plan)
    {
        ALogicalPlanOptimizer.Context ctx = resolver.createContext(context);
        return resolver.optimize(ctx, plan);
    }
}
