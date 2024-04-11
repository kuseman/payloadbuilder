package se.kuseman.payloadbuilder.core.logicalplan.optimization;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.core.utils.CollectionUtils.asSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ISortItem;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.catalog.system.SystemCatalog;
import se.kuseman.payloadbuilder.core.expression.AliasExpression;
import se.kuseman.payloadbuilder.core.expression.AsteriskExpression;
import se.kuseman.payloadbuilder.core.expression.FunctionCallExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralStringExpression;
import se.kuseman.payloadbuilder.core.logicalplan.ConstantScan;
import se.kuseman.payloadbuilder.core.logicalplan.ExpressionScan;
import se.kuseman.payloadbuilder.core.logicalplan.Filter;
import se.kuseman.payloadbuilder.core.logicalplan.ILogicalPlan;
import se.kuseman.payloadbuilder.core.logicalplan.Join;
import se.kuseman.payloadbuilder.core.logicalplan.Limit;
import se.kuseman.payloadbuilder.core.logicalplan.MaxRowCountAssert;
import se.kuseman.payloadbuilder.core.logicalplan.OperatorFunctionScan;
import se.kuseman.payloadbuilder.core.logicalplan.Projection;
import se.kuseman.payloadbuilder.core.logicalplan.Sort;
import se.kuseman.payloadbuilder.core.parser.Location;
import se.kuseman.payloadbuilder.core.statement.LogicalSelectStatement;

/** Test of {@link SubQueryExpressionPushDown} */
public class SubQueryExpressionPushDownTest extends ALogicalPlanOptimizerTest
{
    private final SubQueryExpressionPushDown rule = new SubQueryExpressionPushDown();

    @Test
    public void test_subquery_epxression_with_multiple_column_references()
    {
        String q = """
                SELECT
                (
                  SELECT TOP 1 ar.filename
                  FROM (a.resource) ar
                  WHERE ar.typeid = 'Fs'
                  OR ISNULL(ar.defaultfront, 0) = 1
                  ORDER BY ISNULL(ar.defaultfront, 0)
                ) images
                FROM PDArticle a
                """;

        ILogicalPlan plan = ((LogicalSelectStatement) PARSER.parseQuery(q, new ArrayList<>())
                .getStatements()
                .get(0)).getSelect();

        plan = LogicalPlanOptimizer.optimize(context, plan, new HashMap<>());
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference pdArticle = new TableSourceReference(0, "", QualifiedName.of("PDArticle"), "a");
        TableSourceReference a_resource = new TableSourceReference(1, "", QualifiedName.of("a.resource"), "ar");

        Schema schemaPDArticle = Schema.of(ast("a", ResolvedType.of(Type.Any), pdArticle));
        Schema schemaa_resource = Schema.of(ast("ar", ResolvedType.of(Type.Any), a_resource));

        //@formatter:off
        ILogicalPlan expected =
                projection(
                    new Join(
                        tableScan(schemaPDArticle, pdArticle),
                        new Limit(
                            new Sort(
                                projection(
                                    new Filter(
                                        new ExpressionScan(
                                            a_resource,
                                            schemaa_resource,
                                            ocre("resource", pdArticle, ResolvedType.of(Type.Any)),
                                            null),
                                        null,
                                        or(
                                            eq(cre("typeid", a_resource, ResolvedType.of(Type.Any)), new LiteralStringExpression("Fs")),
                                            eq(new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("isnull"), null,
                                                    asList(cre("defaultfront", a_resource), intLit(0))), intLit(1))
                                        )
                                    ),
                                    asList(
                                        new AliasExpression(cre("filename", a_resource), "__expr0"),
                                        new AliasExpression(cre("defaultfront", a_resource), "defaultfront", true)
                                    )
                                ),
                                asList(sortItem(new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("isnull"), null,
                                                    asList(cre("defaultfront", a_resource, 1, ResolvedType.of(Type.Any)), intLit(0))), ISortItem.Order.ASC))
                            ),
                            intLit(1)),
                        Join.Type.LEFT,
                        null,
                        (IExpression) null,
                        asSet(ast("resource", ResolvedType.of(Type.Any), pdArticle)),
                        false),
                    asList(new AliasExpression(ce("__expr0"), "images")));
        //@formatter:on

        //@formatter:off
        Assertions.assertThat(actual.getSchema())
            .usingRecursiveComparison()
            .ignoringFieldsOfTypes(Location.class, Random.class)
            .isEqualTo(Schema.of(
                    col("images", ResolvedType.of(Type.Any), null)));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        //@formatter:off
        Assertions.assertThat(actual)
            .usingRecursiveComparison()
            .ignoringFieldsOfTypes(Location.class, Random.class)
            .isEqualTo(expected);
        //@formatter:on
        assertEquals(expected, actual);
    }

    @Test
    public void test_operator_function_mixed_with_sub_query_scalar_and_correlation()
    {
        String q = """
                   select a.column,
                   (
                     select
                     (
                       select
                       (
                         select a.col
                         from PDArticle a
                       ) val,
                       (
                         select p.col2
                         from PDProduct p
                         where a.col = p.col4
                       ) val1,
                       a.col5
                       from (bb) bb
                       for object_array
                     )
                   ) values
                   from PDProduct_Article a
                   cross populate apply tableB bb
                """;

        ILogicalPlan plan = getColumnResolvedPlan(q);

        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference tableProductArticle = new TableSourceReference(0, "", QualifiedName.of("PDProduct_Article"), "a");
        TableSourceReference tableArticle = new TableSourceReference(3, "", QualifiedName.of("PDArticle"), "a");
        TableSourceReference tableProduct = new TableSourceReference(4, "", QualifiedName.of("PDProduct"), "p");
        TableSourceReference tableB = new TableSourceReference(1, "", QualifiedName.of("tableB"), "bb");
        TableSourceReference e_bb = new TableSourceReference(2, "", QualifiedName.of("bb"), "bb");

        Schema schemaProductArticle = Schema.of(ast("a", Type.Any, tableProductArticle));
        Schema schemaArticle = Schema.of(ast("a", Type.Any, tableArticle));
        Schema schemaProduct = Schema.of(ast("p", Type.Any, tableProduct));
        Schema schemaTableB = Schema.of(ast("bb", Type.Any, tableB));

        //@formatter:off
        Schema objectArraySchema = Schema.of(
                col("val", ResolvedType.of(Type.Any), null),
                col("val1", ResolvedType.of(Type.Any), null),
                col("col5", ResolvedType.of(Type.Any), tableProductArticle));

        ILogicalPlan expected = 
                projection(
                    new Join(
                        new Join(
                            tableScan(schemaProductArticle, tableProductArticle),
                            tableScan(schemaTableB, tableB),
                            Join.Type.INNER,
                            "bb",
                            (IExpression) null,
                            asSet(),
                            false),
                        new OperatorFunctionScan(Schema.of(Column.of("__expr0", ResolvedType.table(objectArraySchema))),
                            projection(
                               new Join(
                                   new Join(
                                       new Join(
                                           ConstantScan.INSTANCE,
                                           new MaxRowCountAssert(
                                                   projection(
                                                       tableScan(schemaArticle, tableArticle),
                                                       asList(new AliasExpression(cre("col", tableArticle), "__expr2"))),
                                               1),
                                           Join.Type.LEFT,
                                           null,
                                           (IExpression) null,
                                           asSet(),
                                           false),
                                       new ExpressionScan(
                                           e_bb,
                                           Schema.of(ast("bb", ResolvedType.of(Type.Any), e_bb)),
                                           ocre("bb", tableB, ResolvedType.table(schemaTableB)),
                                           null),
                                       Join.Type.LEFT,
                                       null,
                                       (IExpression) null,
                                       asSet(),
                                       true),
                                   new MaxRowCountAssert(
                                       projection(
                                           new Filter(
                                               tableScan(schemaProduct, tableProduct),
                                               null,
                                               eq(ocre("col", tableProductArticle), cre("col4", tableProduct))),
                                           asList(new AliasExpression(cre("col2", tableProduct), "__expr3"))),
                                       1),
                                   Join.Type.LEFT,
                                   null,
                                   (IExpression) null,
                                   asSet(ast("col", Type.Any, tableProductArticle)),
                                   false),
                               asList(new AliasExpression(ce("__expr2"), "val"), new AliasExpression(ce("__expr3"), "val1"), ocre("col5", tableProductArticle))
                            ),
                            "",
                            "object_array",
                            null),
                        Join.Type.LEFT,
                        null,
                        (IExpression) null,
                        asSet(col("bb", ResolvedType.table(schemaTableB), tableB), ast("col", Type.Any, tableProductArticle), ast("col5", Type.Any, tableProductArticle)),
                        false),
                    asList(cre("column", tableProductArticle), new AliasExpression(ce("__expr0", ResolvedType.table(objectArraySchema)), "values")));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_without_for()
    {
        //@formatter:off
        String query = ""
                + "select *, "
                + "( "
                + "  select b.col1 "
                + "  from tableB b "
                + ") values "
                + "from \"table\" ";

        ILogicalPlan plan = getColumnResolvedPlan(query);

        ILogicalPlan actual = optimize(context, plan);
        
        TableSourceReference table = new TableSourceReference(0, "", QualifiedName.of("table"), "");
        TableSourceReference tableB = new TableSourceReference(1, "", QualifiedName.of("tableB"), "b");
        Schema schema = Schema.of(ast("", Type.Any, table));
        Schema schemaB = Schema.of(ast("b", Type.Any, tableB));

        //@formatter:off
        ILogicalPlan expected = 
                projection(
                    new Join(
                        new Join(
                            ConstantScan.INSTANCE,
                            new MaxRowCountAssert(
                                projection(
                                    tableScan(schemaB, tableB),
                                asList(new AliasExpression(cre("col1", tableB), "__expr0"))),
                            1),
                            Join.Type.LEFT,
                            null,
                            (IExpression) null,
                            asSet(),
                            false),
                        tableScan(schema, table),
                        Join.Type.LEFT,
                        null,
                        (IExpression) null,
                        asSet(),
                        true),
                    asList(new AsteriskExpression(QualifiedName.EMPTY, null, Set.of(table)), new AliasExpression(ce("__expr0"), "values")));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        //@formatter:off
        Assertions.assertThat(actual.getSchema())
            .usingRecursiveComparison()
            .ignoringFieldsOfTypes(Location.class, Random.class)
            .isEqualTo(Schema.of(
                new CoreColumn("", ResolvedType.of(Type.Any), "*", false, table, CoreColumn.Type.ASTERISK),
                new CoreColumn("values", ResolvedType.of(Type.Any), "", false, null, CoreColumn.Type.REGULAR)));

        // Validate the schema below the projection. The schema must stay consistent even if we switch outer/inner
        Assertions.assertThat(((Projection) actual).getInput()
                .getSchema())
            .usingRecursiveComparison()
            .ignoringFieldsOfTypes(Location.class, Random.class)
            .isEqualTo(Schema.of(
                    new CoreColumn("", ResolvedType.of(Type.Any), "", false, table, CoreColumn.Type.ASTERISK),
                    col("__expr0", Type.Any, tableB)));
        //@formatter:on

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_multiple_without_for()
    {
        //@formatter:off
        String query = ""
                + "select *, "
                + "( "
                + "  select b.col1 "
                + "  from tableB b "
                + ") values, "
                + "( "
                + "  select c.col2 "
                + "  from tableC c "
                + ") otherValues "
                + "from \"table\" ";

        ILogicalPlan plan = getColumnResolvedPlan(query);

        ILogicalPlan actual = optimize(context, plan);
        
        TableSourceReference table = new TableSourceReference(0, "", QualifiedName.of("table"), "");
        TableSourceReference tableB = new TableSourceReference(1, "", QualifiedName.of("tableB"), "b");
        TableSourceReference tableC = new TableSourceReference(2, "", QualifiedName.of("tableC"), "c");
        Schema schema = Schema.of(ast("", Type.Any, table));
        Schema schemaB = Schema.of(ast("b", Type.Any, tableB));
        Schema schemaC = Schema.of(ast("c", Type.Any, tableC));

        //@formatter:off
        ILogicalPlan expected = 
                projection(
                    new Join(
                        new Join(
                            ConstantScan.INSTANCE,
                            new MaxRowCountAssert(
                                projection(
                                    tableScan(schemaC, tableC),
                                    asList(new AliasExpression(cre("col2", tableC), "__expr1"))),
                            1),
                            Join.Type.LEFT,
                            null,
                            (IExpression) null,
                            asSet(),
                            false),
                        new Join(
                            new Join(
                                ConstantScan.INSTANCE,
                                new MaxRowCountAssert(
                                    projection(
                                        tableScan(schemaB, tableB),
                                        asList(new AliasExpression(cre("col1", tableB), "__expr0"))),
                                1),
                                Join.Type.LEFT,
                                null,
                                (IExpression) null,
                                asSet(),
                                false),
                            tableScan(schema, table),
                            Join.Type.LEFT,
                            null,
                            (IExpression) null,
                            asSet(),
                            true),
                        Join.Type.LEFT,
                        null,
                        (IExpression) null,
                        asSet(),
                        true),
                    asList(new AsteriskExpression(QualifiedName.EMPTY, null, Set.of(table)), new AliasExpression(ce("__expr0"), "values"), new AliasExpression(ce("__expr1"), "otherValues") ) );
        //@formatter:on

        //@formatter:off
        Assertions.assertThat(actual.getSchema())
            .usingRecursiveComparison()
            .ignoringFieldsOfTypes(Location.class, Random.class)
            .isEqualTo(Schema.of(
                new CoreColumn("", ResolvedType.of(Type.Any), "*", false, table, CoreColumn.Type.ASTERISK),
                col("values", Type.Any, null),
                col("otherValues", Type.Any, null)));

        // Validate the schema below the projection
        Assertions.assertThat( ((Projection) actual).getInput()
                .getSchema())
        .usingRecursiveComparison()
        .ignoringFieldsOfTypes(Location.class, Random.class)
        .isEqualTo(Schema.of(
                new CoreColumn("", ResolvedType.of(Type.Any), "", false, table, CoreColumn.Type.ASTERISK),
                col("__expr0", Type.Any, tableB),
                col("__expr1", Type.Any, tableC)));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_multiple_correlated_without_for()
    {
        //@formatter:off
        String query = ""
                + "select *, "
                + "( "
                + "  select b.col1 "
                + "  from tableB b "
                + "  where b.col2 = a.col2 "
                + ") values, "
                + "( "
                + "  select c.col2 "
                + "  from tableC c "
                + ") otherValues "
                + "from \"table\" a ";

        ILogicalPlan plan = getColumnResolvedPlan(query);

        ILogicalPlan actual = optimize(context, plan);
        
        TableSourceReference table = new TableSourceReference(0, "", QualifiedName.of("table"), "a");
        TableSourceReference tableB = new TableSourceReference(1, "", QualifiedName.of("tableB"), "b");
        TableSourceReference tableC = new TableSourceReference(2, "", QualifiedName.of("tableC"), "c");
        Schema schema = Schema.of(ast("a", Type.Any, table));
        Schema schemaB = Schema.of(ast("b", Type.Any, tableB));
        Schema schemaC = Schema.of(ast("c", Type.Any, tableC));

        //@formatter:off
        ILogicalPlan expected = 
                projection(
                    new Join(
                        new Join(
                            ConstantScan.INSTANCE,
                            new MaxRowCountAssert(
                                projection(
                                    tableScan(schemaC, tableC),
                                    asList(new AliasExpression(cre("col2", tableC), "__expr1"))),
                            1),
                            Join.Type.LEFT,
                            null,
                            (IExpression) null,
                            asSet(),
                            false),
                        new Join(
                            tableScan(schema, table),               // Correlated sub query below so this must be outer
                            new MaxRowCountAssert(
                                projection(
                                    new Filter(
                                        tableScan(schemaB, tableB),
                                        null,
                                        eq(cre("col2", tableB), ocre("col2", table))),
                                    asList(new AliasExpression(cre("col1", tableB), "__expr0"))),
                            1),
                            Join.Type.LEFT,
                            null,
                            (IExpression) null,
                            asSet(ast("col2", Type.Any, table)),
                            false),
                        Join.Type.LEFT,
                        null,
                        (IExpression) null,
                        asSet(),
                        true),
                    asList(new AsteriskExpression(QualifiedName.EMPTY, null, Set.of(table)), new AliasExpression(ce("__expr0"), "values"), new AliasExpression(ce("__expr1"), "otherValues") ) );

        //@formatter:off
        Assertions.assertThat(actual.getSchema())
            .usingRecursiveComparison()
            .ignoringFieldsOfTypes(Location.class, Random.class)
            .isEqualTo(Schema.of(
                new CoreColumn("", ResolvedType.of(Type.Any), "*", false, table, CoreColumn.Type.ASTERISK),
                col("values", Type.Any, null),
                col("otherValues", Type.Any, null)));

        // Validate the schema below the projection. The schema must stay consistent even if we switch outer/inner
        Assertions.assertThat( ((Projection) actual).getInput()
                .getSchema())
        .usingRecursiveComparison()
        .ignoringFieldsOfTypes(Location.class, Random.class)
        .isEqualTo(Schema.of(
                new CoreColumn("a", ResolvedType.of(Type.Any), "a", false, table, CoreColumn.Type.ASTERISK),
                col("__expr0", Type.Any, tableB),
                col("__expr1", Type.Any, tableC)));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_multiple_correlated_without_for_2()
    {
        //@formatter:off
        String query = ""
                + "select *, "
                + "( "
                + "  select c.col2 "
                + "  from tableC c "
                + ") otherValues, "
                + "( "
                + "  select b.col1 "
                + "  from tableB b "
                + "  where b.col2 = a.col2 "          // a.col2 correlated
                + ") values "
                + "from \"table\" a ";

        ILogicalPlan plan = getColumnResolvedPlan(query);

        ILogicalPlan actual = optimize(context, plan);
        
        TableSourceReference table = new TableSourceReference(0, "", QualifiedName.of("table"), "a");
        TableSourceReference tableB = new TableSourceReference(2, "", QualifiedName.of("tableB"), "b");
        TableSourceReference tableC = new TableSourceReference(1, "", QualifiedName.of("tableC"), "c");
        Schema schema = Schema.of(ast("a", Type.Any, table));
        Schema schemaB = Schema.of(ast("b", Type.Any, tableB));
        Schema schemaC = Schema.of(ast("c", Type.Any, tableC));

        //@formatter:off
        ILogicalPlan expected = 
                projection(
                    new Join(
                        new Join(
                            new Join(
                                ConstantScan.INSTANCE,
                                new MaxRowCountAssert(
                                    projection(
                                        tableScan(schemaC, tableC),
                                        asList(new AliasExpression(cre("col2", tableC), "__expr0"))),
                                1),
                                Join.Type.LEFT,
                                null,
                                (IExpression) null,
                                asSet(),
                                false),
                            tableScan(schema, table),
                            Join.Type.LEFT,
                            null,
                            (IExpression) null,
                            asSet(),
                            true),
                        new MaxRowCountAssert(
                            projection(
                                new Filter(
                                    tableScan(schemaB, tableB),
                                    null,
                                    eq(cre("col2", tableB), ocre("col2", table))),
                                asList(new AliasExpression(cre("col1", tableB), "__expr1"))),
                        1),
                        Join.Type.LEFT,
                        null,
                        (IExpression) null,
                        asSet(ast("col2", Type.Any, table)),
                        false),
                    asList(new AsteriskExpression(QualifiedName.EMPTY, null, Set.of(table)), new AliasExpression(ce("__expr0"), "otherValues"), new AliasExpression(ce("__expr1"), "values")));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_multiple_correlated_without_for_3()
    {
        //@formatter:off
        String query = ""
                + "select *, "
                + "( "
                + "  select c.col2 "
                + "  from tableC c "
                + "  where c.col3 = a.col3 "          // a.col3 correlated
                + ") otherValues, "
                + "( "
                + "  select b.col1 "
                + "  from tableB b "
                + "  where b.col2 = a.col2 "          // a.col2 correlated
                + ") values "
                + "from \"table\" a ";

        ILogicalPlan plan = getColumnResolvedPlan(query);

        ILogicalPlan actual = optimize(context, plan);
        
        TableSourceReference table = new TableSourceReference(0, "", QualifiedName.of("table"), "a");
        TableSourceReference tableB = new TableSourceReference(2, "", QualifiedName.of("tableB"), "b");
        TableSourceReference tableC = new TableSourceReference(1, "", QualifiedName.of("tableC"), "c");
        Schema schema = Schema.of(ast("a", Type.Any, table));
        Schema schemaB = Schema.of(ast("b", Type.Any, tableB));
        Schema schemaC = Schema.of(ast("c", Type.Any, tableC));

        //@formatter:off
        ILogicalPlan expected = 
                projection(
                    new Join(
                        new Join(
                            tableScan(schema, table),
                            new MaxRowCountAssert(
                                projection(
                                    new Filter(
                                        tableScan(schemaC, tableC),
                                        null,
                                        eq(cre("col3", tableC), ocre("col3", table))),
                                    asList(new AliasExpression(cre("col2", tableC), "__expr0"))),
                            1),
                            Join.Type.LEFT,
                            null,
                            (IExpression) null,
                            asSet(ast("col3", Type.Any, table)),
                            false),
                        new MaxRowCountAssert(
                            projection(
                                new Filter(
                                    tableScan(schemaB, tableB),
                                    null,
                                    eq(cre("col2", tableB), ocre("col2", table))),
                                asList(new AliasExpression(cre("col1", tableB), "__expr1"))),
                        1),
                        Join.Type.LEFT,
                        null,
                        (IExpression) null,
                        asSet(ast("col2", Type.Any, table)),
                        false),
                    asList(new AsteriskExpression(QualifiedName.EMPTY, null, Set.of(table)), new AliasExpression(ce("__expr0"), "otherValues"), new AliasExpression(ce("__expr1"), "values")));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));
        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_complex_sub_queries_without_for()
    {
        // Here we have a arithmetic expression where one side is a subquery
        //@formatter:off
        String query = ""
                + "select *, "
                + "( "
                + "  select b.col1 "
                + "  from tableB b "
                + ") + 1 values "
                + "from \"table\" ";

        ILogicalPlan plan = getColumnResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);
        
        TableSourceReference table = new TableSourceReference(0, "", QualifiedName.of("table"), "");
        TableSourceReference tableB = new TableSourceReference(1, "", QualifiedName.of("tableB"), "b");
        Schema schema = Schema.of(ast("", Type.Any, table));
        Schema schemaB = Schema.of(ast("b", Type.Any, tableB));
        
        //@formatter:off
        ILogicalPlan expected = 
                projection(
                    new Join(
                        new Join(
                            ConstantScan.INSTANCE,
                            new MaxRowCountAssert(
                                projection(
                                    tableScan(schemaB, tableB),
                                    asList(new AliasExpression(cre("col1", tableB), "__expr0"))),
                            1),
                            Join.Type.LEFT,
                            null,
                            (IExpression) null,
                            asSet(),
                            false),
                        tableScan(schema, table),
                        Join.Type.LEFT,
                        null,
                        (IExpression) null,
                        asSet(),
                        true),
                    asList(new AsteriskExpression(QualifiedName.EMPTY, null, Set.of(table)), new AliasExpression(add(ce("__expr0"), intLit(1)), "values")));
        //@formatter:on

        // System.out.println(actual.print(0));
        // System.out.println(expected.print(0));

        assertEquals(expected, actual);
    }

    @Test
    public void test_nested_sub_queries_without_for_gets_unnested()
    {
        //@formatter:off
        String query = ""
                + "select *, "
                + "( "
                + "  select "
                + "  ( "
                + "    select b.col1 "
                + "    from tableB b "
                + "  ) + 1 innerValues "        // Alias innerValues is removed here since that column is never used
                + ") values "
                + "from \"table\" ";

        ILogicalPlan plan = getColumnResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);
        
        TableSourceReference table = new TableSourceReference(0, "", QualifiedName.of("table"), "");
        TableSourceReference tableB = new TableSourceReference(1, "", QualifiedName.of("tableB"), "b");
        Schema schema = Schema.of(ast("", Type.Any, table));
        Schema schemaB = Schema.of(ast("b", Type.Any, tableB));
        
        //@formatter:off
        ILogicalPlan expected = 
                projection(
                    new Join(
                        new Join(
                            ConstantScan.INSTANCE,
                            new MaxRowCountAssert(
                                projection(
                                        tableScan(schemaB, tableB),
                                    asList(new AliasExpression(add(cre("col1", tableB), intLit(1)), "__expr0"))),
                            1),
                            Join.Type.LEFT,
                            null,
                            (IExpression) null,
                            asSet(),
                            false),
                        tableScan(schema, table),
                        Join.Type.LEFT,
                        null,
                        (IExpression) null,
                        asSet(),
                        true),
                    asList(new AsteriskExpression(QualifiedName.EMPTY, null, Set.of(table)), new AliasExpression(ce("__expr0", ResolvedType.of(Type.Int)), "values")));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_correlated_sub_queries_without_for()
    {
        //@formatter:off
        String query = ""
                + "select *, "
                + "( "
                + "  select b.col1 "
                + "  from tableB b "
                + "  where b.col2 > a.col2 "      // a.col2 outer reference
                + ") values "
                + "from \"table\" a ";

        ILogicalPlan plan = getColumnResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);
        
        TableSourceReference table = new TableSourceReference(0, "", QualifiedName.of("table"), "a");
        TableSourceReference tableB = new TableSourceReference(1, "", QualifiedName.of("tableB"), "b");
        Schema schema = Schema.of(ast("a", Type.Any, table));
        Schema schemaB = Schema.of(ast("b", Type.Any, tableB));
        
        //@formatter:off
        ILogicalPlan expected = 
                projection(
                    new Join(
                        tableScan(schema, table),
                        new MaxRowCountAssert(
                            projection(
                                new Filter(
                                    tableScan(schemaB, tableB),
                                    null,
                                    gt(cre("col2", tableB), ocre("col2", table))),
                                asList(new AliasExpression(cre("col1", tableB), "__expr0"))),
                        1),
                        Join.Type.LEFT,
                        null,
                        (IExpression) null,
                        asSet(ast("col2", Type.Any, table)),
                        false),
                    asList(new AsteriskExpression(QualifiedName.EMPTY, null, Set.of(table)), new AliasExpression(ce("__expr0"), "values")));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        assertEquals(expected, actual);
    }

    @Test
    public void test_arithmetics_between_two_sub_query_expressions_one_correlated_the_other_not()
    {
        //@formatter:off
        String query = ""
                + "select *, "
                + "( "
                + "  select b.col1 "
                + "  from tableB b "
                + "  where b.col2 > a.col2 "      // a.col2 outer reference
                + ") +"
                + "( "
                + "  select c.col2 "
                + "  from tableC c "
                + ") value "
                + "from \"table\" a ";

        ILogicalPlan plan = getColumnResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);
        
        TableSourceReference table = new TableSourceReference(0, "", QualifiedName.of("table"), "a");
        TableSourceReference tableB = new TableSourceReference(1, "", QualifiedName.of("tableB"), "b");
        TableSourceReference tableC = new TableSourceReference(2, "", QualifiedName.of("tableC"), "c");
        Schema schema = Schema.of(ast("a", Type.Any, table));
        Schema schemaB = Schema.of(ast("b", Type.Any, tableB));
        Schema schemaC = Schema.of(ast("c", Type.Any, tableC));
        
        //@formatter:off
        ILogicalPlan expected = 
                projection(
                    new Join(
                        new Join(
                            ConstantScan.INSTANCE,
                            new MaxRowCountAssert(
                                projection(                                 // Non correlated sub query is placed outer
                                    tableScan(schemaC, tableC), 
                                    asList(new AliasExpression(cre("col2", tableC), "__expr1"))),
                            1),
                            Join.Type.LEFT,
                            null,
                            (IExpression) null,
                            asSet(),
                            false),
                        new Join(
                            tableScan(schema, table),
                            new MaxRowCountAssert(
                                projection(                             // Correlated sub query then it needs to be inner
                                    new Filter(
                                        tableScan(schemaB, tableB),
                                        null,
                                        gt(cre("col2", tableB), ocre("col2", table))),
                                    asList(new AliasExpression(cre("col1", tableB), "__expr0"))),
                            1),
                            Join.Type.LEFT,
                            null,
                            (IExpression) null,
                            asSet(ast("col2", Type.Any, table)),
                            false),
                        Join.Type.LEFT,
                        null,
                        (IExpression) null,
                        asSet(),
                        true),
                    asList(new AsteriskExpression(QualifiedName.EMPTY, null, Set.of(table)), new AliasExpression(add(ce("__expr0"), ce("__expr1")), "value")));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        assertEquals(expected, actual);
    }

    private ILogicalPlan optimize(IExecutionContext context, ILogicalPlan plan)
    {
        ALogicalPlanOptimizer.Context ctx = rule.createContext(context);
        return rule.optimize(ctx, plan);
    }
}
