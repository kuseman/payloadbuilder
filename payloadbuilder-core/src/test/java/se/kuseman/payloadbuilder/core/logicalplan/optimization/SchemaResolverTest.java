package se.kuseman.payloadbuilder.core.logicalplan.optimization;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

import java.util.List;
import java.util.Random;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.DatasourceData.Projection;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableSchema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.expression.AliasExpression;
import se.kuseman.payloadbuilder.core.expression.AsteriskExpression;
import se.kuseman.payloadbuilder.core.expression.UnresolvedSubQueryExpression;
import se.kuseman.payloadbuilder.core.logicalplan.ConstantScan;
import se.kuseman.payloadbuilder.core.logicalplan.ILogicalPlan;
import se.kuseman.payloadbuilder.core.logicalplan.Limit;
import se.kuseman.payloadbuilder.core.logicalplan.OperatorFunctionScan;
import se.kuseman.payloadbuilder.core.logicalplan.TableScan;
import se.kuseman.payloadbuilder.core.parser.Location;
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

        TableSourceReference tableSource = new TableSourceReference(0, TableSourceReference.Type.TABLE, "sys", QualifiedName.of("functions"), "");

        Schema expectedSchema = Schema.of(col("name", Type.String, tableSource), col("type", Type.String, tableSource), col("description", Type.String, tableSource));

        //@formatter:off
        ILogicalPlan expected =
                projection(
                    new TableScan(new TableSchema(expectedSchema), tableSource, Projection.ALL, emptyList(), null),
                    List.of(new AsteriskExpression(null)));
        //@formatter:on

        Assertions.assertThat(actual.getSchema())
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expectedSchema);

        assertEquals(expected, actual);
    }

    @Test
    public void test_table_source_schema_with_top()
    {
        ILogicalPlan plan = s("select top 10 * from sys#functions");
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference tableSource = new TableSourceReference(0, TableSourceReference.Type.TABLE, "sys", QualifiedName.of("functions"), "");
        Schema expectedSchema = Schema.of(col("name", Type.String, tableSource), col("type", Type.String, tableSource), col("description", Type.String, tableSource));

        //@formatter:off
        ILogicalPlan expected =
                new Limit(
                        projection(
                            new TableScan(new TableSchema(expectedSchema), tableSource, Projection.ALL, emptyList(), null),
                            List.of(new AsteriskExpression(null))),
                        e("10"));
        //@formatter:on

        Assertions.assertThat(actual.getSchema())
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expectedSchema);

        assertEquals(expected, actual);
    }

    @Test
    public void test_function_resolving()
    {
        ILogicalPlan plan = s("select concat('hello', 123)");
        ILogicalPlan actual = optimize(context, plan);

        Schema expectedSchema = Schema.of(col(ResolvedType.of(Type.String), "concat('hello', 123)"));
        assertEquals(expectedSchema, actual.getSchema());
    }

    @Test
    public void test_function_resolving_aggregate()
    {
        ILogicalPlan plan = s("select sum(col2) from \"table\" group by col");
        ILogicalPlan actual = optimize(context, plan);

        Schema expectedSchema = Schema.of(col(ResolvedType.of(Type.Any), "sum(col2)"));
        assertEquals(expectedSchema, actual.getSchema());
    }

    @Test
    public void test_function_resolving_aggregate_nesting_of_scalar_aggregates()
    {
        ILogicalPlan plan = s("select sum(sum(col2)) from \"table\" group by col");
        ILogicalPlan actual = optimize(context, plan);

        Schema expectedSchema = Schema.of(col(ResolvedType.of(Type.Any), "sum(sum(col2))"));
        assertEquals(expectedSchema, actual.getSchema());
    }

    @Test
    public void test_nested_aggregate_functions()
    {
        ILogicalPlan plan = s("select object_array(object_array('col')) from \"table\" group by col");

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
        ILogicalPlan plan = s("select sum(col2, 10) from \"table\" group by col");

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

        TableSourceReference tableSource = new TableSourceReference(0, TableSourceReference.Type.TABLE, "sys", QualifiedName.of("tables"), "");
        Schema expectedSchema = Schema.of(col("name", Type.String, tableSource), col("schema", Type.String, tableSource), col("indices", Type.String, tableSource), col("rows", Type.Int, tableSource));

        //@formatter:off
        ILogicalPlan expected = ConstantScan.create(
                        asList(
                                intLit(12345),
                                new AliasExpression(
                                    new UnresolvedSubQueryExpression(
                                        new OperatorFunctionScan(
                                           Schema.of(Column.of("output", Type.Any)),
                                           projection(
                                               new TableScan(new TableSchema(expectedSchema), tableSource, Projection.ALL, emptyList(), null),
                                               List.of(new AsteriskExpression(null))),
                                           "",
                                           "object_array",
                                           null
                                        ), null),
                                    "tables")),
                        null);
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    private ILogicalPlan optimize(IExecutionContext context, ILogicalPlan plan)
    {
        ALogicalPlanOptimizer.Context ctx = resolver.createContext(context);
        return resolver.optimize(ctx, plan);
    }
}
