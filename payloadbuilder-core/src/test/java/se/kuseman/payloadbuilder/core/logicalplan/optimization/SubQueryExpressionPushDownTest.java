package se.kuseman.payloadbuilder.core.logicalplan.optimization;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.core.utils.CollectionUtils.asSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ISortItem;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.catalog.system.SystemCatalog;
import se.kuseman.payloadbuilder.core.expression.AggregateWrapperExpression;
import se.kuseman.payloadbuilder.core.expression.AliasExpression;
import se.kuseman.payloadbuilder.core.expression.AsteriskExpression;
import se.kuseman.payloadbuilder.core.expression.FunctionCallExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralBooleanExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralStringExpression;
import se.kuseman.payloadbuilder.core.logicalplan.Aggregate;
import se.kuseman.payloadbuilder.core.logicalplan.Concatenation;
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
    public void test_subquery_epxression_with_table_value_ctor_more_than_one_row()
    {
        String q = """
                SELECT (
                    select col1
                    from
                    (
                        values (1, 2), (3,4)
                    ) y (col1, col2)
                )
                """;

        ILogicalPlan plan = getSchemaResolvedPlan(q);
        try
        {
            optimize(context, plan);
            fail();
        }
        catch (se.kuseman.payloadbuilder.core.parser.ParseException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("A sub query expression must return a single row"));
        }
    }

    @Test
    public void test_tables_values_ctor_in_projection()
    {
        String q = """
                SELECT
                (
                    select x.col
                    from
                    (
                        values (1)
                    ) x (col)
                )

                """;

        ILogicalPlan plan = getSchemaResolvedPlan(q);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference subQueryX = new TableSourceReference(0, TableSourceReference.Type.SUBQUERY, "", QualifiedName.of("x"), "x");

        //@formatter:off
        ILogicalPlan expected = projection(
                new Join(
                    ConstantScan.ONE_ROW_EMPTY_SCHEMA,
                    projection(
                        subQuery(
                            ConstantScan.create(subQueryX, List.of("col"), List.of(List.of(intLit(1))), null),
                            subQueryX),
                        asList(new AliasExpression(uce("x", "col"), "__expr0", true))
                    ),
                    Join.Type.LEFT,
                    null,
                    null,
                    null,
                    false,
                    Schema.EMPTY),
                asList(uce("__expr0")));
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
    public void test_subquery_epxression_within_table_value_ctor()
    {
        String q = """
                SELECT *
                FROM
                (
                    VALUES (1, (Select 'hello' col1 for object))
                    ,      (2, (Select 'world' col2 for object))

                ) x (col1, col2)

                """;

        ILogicalPlan plan = getSchemaResolvedPlan(q);
        ILogicalPlan actual = optimize(context, plan);
        TableSourceReference subQueryX = new TableSourceReference(0, TableSourceReference.Type.SUBQUERY, "", QualifiedName.of("x"), "x");

        //@formatter:off
        ILogicalPlan expected = 
            projection(
                subQuery(
                    new Concatenation(
                        List.of("col1", "col2"),
                        List.of(
                        projection(
                            new OperatorFunctionScan(
                                Schema.of(new CoreColumn("__expr0", ResolvedType.ANY, "", true)),
                                ConstantScan.create(List.of(new AliasExpression(stringLit("hello"), "col1")), null),
                                "",
                                "object",
                                null),
                            List.of(intLit(1), uce("__expr0"))),
                        projection(
                            new OperatorFunctionScan(
                                Schema.of(new CoreColumn("__expr1", ResolvedType.ANY, "", true)),
                                ConstantScan.create(List.of(new AliasExpression(stringLit("world"), "col2")), null),
                                "",
                                "object",
                                null),
                            List.of(intLit(2), uce("__expr1")))
                        ), null),
                    subQueryX),
                List.of(new AsteriskExpression(null)));
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

        plan = getSchemaResolvedPlan(q);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference pdArticle = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("PDArticle"), "a");
        TableSourceReference a_resource = new TableSourceReference(1, TableSourceReference.Type.EXPRESSION, "", QualifiedName.of("a.resource"), "ar");

        Schema schemaPDArticle = Schema.of(ast("a", pdArticle));

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
                                            Schema.EMPTY,
                                            uce("a", "resource"),
                                            null),
                                        null,
                                        or(
                                            eq(uce("ar", "typeid"), new LiteralStringExpression("Fs")),
                                            eq(new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("isnull"), null,
                                                    asList(uce("ar", "defaultfront"), intLit(0))), intLit(1))
                                        )
                                    ),
                                    asList(new AliasExpression(uce("ar", "filename"), "__expr0", true))
                                ),
                                asList(sortItem(new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("isnull"), null,
                                                    asList(uce("ar", "defaultfront"), intLit(0))), ISortItem.Order.ASC))
                            ),
                            intLit(1)),
                        Join.Type.LEFT,
                        null,
                        (IExpression) null,
                        Set.of(),
                        false,
                        Schema.EMPTY),
                    asList(new AliasExpression(uce("__expr0"), "images")));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        //@formatter:off
        Assertions.assertThat(actual.getSchema())
            .usingRecursiveComparison()
            .ignoringFieldsOfTypes(Location.class, Random.class)
            .isEqualTo(Schema.of(
                    col("images", ResolvedType.of(Type.Any), null)));
        //@formatter:on

        //@formatter:off
        Assertions.assertThat(actual)
            .usingRecursiveComparison()
            .ignoringFieldsOfTypes(Location.class, Random.class)
            .isEqualTo(expected);
        //@formatter:on
        assertEquals(expected, actual);
    }

    @Test
    public void test_operator_function_with_nested_sub_query()
    {
        String q = """
                SELECT
                (
                    SELECT c.name
                    ,      c.id
                    , (
                        SELECT t2.*
                        FROM (c.tbl) t
                        CROSS APPLY (t.tbl2) t2
                        WHERE t2.languagecode = 'sv'
                        FOR OBJECT_ARRAY
                      ) fields
                    FOR OBJECT
                )               category
                FROM product p
                INNER JOIN category c
                  on c.id = p.categoryId
                """;

        ILogicalPlan plan = getSchemaResolvedPlan(q);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference tableProduct = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("product"), "p");
        TableSourceReference tableCategory = new TableSourceReference(1, TableSourceReference.Type.TABLE, "", QualifiedName.of("category"), "c");
        TableSourceReference t = new TableSourceReference(2, TableSourceReference.Type.EXPRESSION, "", QualifiedName.of("c.tbl"), "t");
        TableSourceReference t2 = new TableSourceReference(3, TableSourceReference.Type.EXPRESSION, "", QualifiedName.of("t.tbl2"), "t2");

        Schema schemaProduct = Schema.of(ast("p", tableProduct));
        Schema schemaCategory = Schema.of(ast("c", tableCategory));

        //@formatter:off
        ILogicalPlan expected = projection(
                new Join(
                    new Join(
                        tableScan(schemaProduct, tableProduct),
                        tableScan(schemaCategory, tableCategory),
                        Join.Type.INNER,
                        null,
                        eq(uce("c", "id"), uce("p", "categoryId")),
                        Set.of(),
                        false,
                        Schema.EMPTY),
                    new OperatorFunctionScan(
                        Schema.of(col("__expr0", ResolvedType.of(Type.Any), null, true)),
                        projection(
                            new Join(
                                ConstantScan.ONE_ROW_EMPTY_SCHEMA,
                                new OperatorFunctionScan(
                                    Schema.of(col("__expr1", ResolvedType.of(Type.Any), null, true)),
                                    projection(
                                        new Filter(
                                            new Join(
                                                new ExpressionScan(t, Schema.EMPTY, uce("c", "tbl"), null),
                                                new ExpressionScan(t2, Schema.EMPTY, uce("t", "tbl2"), null),
                                                Join.Type.INNER,
                                                null,
                                                null,
                                                Set.of(),
                                                false,
                                                Schema.EMPTY),
                                            null,
                                            eq(uce("t2", "languagecode"), new LiteralStringExpression("sv"))),
                                        List.of(new AsteriskExpression(QualifiedName.of("t2"), null, Set.of()))),
                                    "",
                                    "OBJECT_ARRAY",
                                    null),
                                Join.Type.LEFT,
                                null,
                                null,
                                Set.of(),
                                false,
                                Schema.EMPTY),
                            List.of(uce("c", "name"),
                                    uce("c", "id"),
                                    new AliasExpression(uce("__expr1"), "fields"))),
                        "",
                        "OBJECT",
                        null),
                    Join.Type.LEFT,
                    null,
                    null,
                    Set.of(),
                    false,
                    Schema.EMPTY),
                List.of(new AliasExpression(uce("__expr0"), "category")));
        //@formatter:on

        // System.out.println(actual.print(0));
        // System.out.println(expected.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

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
                     ) "values1"                        -- Alias that is removed due to being nested
                   ) "values"
                   from PDProduct_Article a
                   cross populate apply tableB bb
                """;

        ILogicalPlan plan = getSchemaResolvedPlan(q);

        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference tableProductArticle = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("PDProduct_Article"), "a");
        TableSourceReference tableArticle = new TableSourceReference(3, TableSourceReference.Type.TABLE, "", QualifiedName.of("PDArticle"), "a");
        TableSourceReference tableProduct = new TableSourceReference(4, TableSourceReference.Type.TABLE, "", QualifiedName.of("PDProduct"), "p");
        TableSourceReference tableB = new TableSourceReference(1, TableSourceReference.Type.TABLE, "", QualifiedName.of("tableB"), "bb");
        TableSourceReference e_bb = new TableSourceReference(2, TableSourceReference.Type.EXPRESSION, "", QualifiedName.of("bb"), "bb");

        Schema schemaProductArticle = Schema.of(ast("a", tableProductArticle));
        Schema schemaArticle = Schema.of(ast("a", tableArticle));
        Schema schemaProduct = Schema.of(ast("p", tableProduct));
        Schema schemaTableB = Schema.of(ast("bb", tableB));

        //@formatter:off
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
                            false,
                            Schema.EMPTY),
                        new OperatorFunctionScan(
                                Schema.of(col("__expr0", ResolvedType.ANY, null, true)),
                                projection(
                                   new Join(
                                       new Join(
                                           new ExpressionScan(
                                               e_bb,
                                               Schema.EMPTY,
                                               uce("bb"),
                                               null),
                                           new MaxRowCountAssert(
                                               projection(
                                                   tableScan(schemaArticle, tableArticle),
                                                   asList(new AliasExpression(uce("a", "col"), "__expr1", true))),
                                               1),
                                           Join.Type.LEFT,
                                           null,
                                           (IExpression) null,
                                           asSet(),
                                           false,
                                           Schema.EMPTY),
                                       new MaxRowCountAssert(
                                           projection(
                                               new Filter(
                                                   tableScan(schemaProduct, tableProduct),
                                                   null,
                                                   eq(uce("a", "col"), uce("p", "col4"))),
                                               asList(new AliasExpression(uce("p", "col2"), "__expr2", true))),
                                           1),
                                       Join.Type.LEFT,
                                       null,
                                       (IExpression) null,
                                       Set.of(),
                                       false,
                                       Schema.EMPTY),
                                   asList(new AliasExpression(uce("__expr1"), "val"),
                                           new AliasExpression(uce("__expr2"), "val1"), uce("a", "col5"))
                                ),
                                "",
                                "object_array",
                                null),
                        Join.Type.LEFT,
                        null,
                        (IExpression) null,
                        Set.of(),
                        false,
                        Schema.EMPTY),
                    asList(uce("a", "column"),
                            new AliasExpression(uce("__expr0"), "values")));
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
    public void test_sub_query_push_down_with_ordinals()
    {
        //@formatter:off
        String query = ""
                + "select col3, "
                + "( "
                + "  select 1 col1, true col2 "
                + "  for object "
                + ") \"values\" "
                + "from \"stableA\" a";
        //@formatter:on
        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected =
                projection(
                    new Join(
                        tableScan(schemaSTableA, sTableA),
                        new OperatorFunctionScan(
                            Schema.of(col("__expr0", ResolvedType.ANY, null, true)),
                            ConstantScan.create(List.of(
                                    new AliasExpression(intLit(1), "col1"), new AliasExpression(LiteralBooleanExpression.TRUE, "col2")
                            ), null),
                            "",
                            "object",
                            null),
                        Join.Type.LEFT,
                        null,
                        (IExpression) null,
                        asSet(),
                        false,
                        Schema.EMPTY),
                    asList(uce("col3"), new AliasExpression(uce("__expr0"), "values")));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        //@formatter:off
        Assertions.assertThat(actual.getSchema())
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(Schema.of(col("col3", Type.Any),
                        col("values", ResolvedType.ANY)));

        // Validate the schema below the projection. The schema must stay consistent even if we switch outer/inner
        Assertions.assertThat(((Projection) actual).getInput()
                .getSchema())
            .usingRecursiveComparison()
            .ignoringFieldsOfTypes(Location.class, Random.class)
            .isEqualTo(Schema.of(
                    col("col1", Type.Int, sTableA),
                    col("col2", Type.String, sTableA),
                    col("col3", Type.Float, sTableA),
                    col("__expr0", ResolvedType.ANY, null, true)));
        //@formatter:on

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_sub_query_push_down_with_ordinals_no_table_source()
    {
        //@formatter:off
        String query = ""
                + "select 1 col3, "
                + "( "
                + "  select 1 col1, true col2 "
                + "  for object "
                + ") \"values\" ";
        //@formatter:on
        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected =
                projection(
                    new OperatorFunctionScan(
                            Schema.of(col("__expr0", ResolvedType.ANY, null, true)),
                            ConstantScan.create(List.of(
                                    new AliasExpression(intLit(1), "col1"), new AliasExpression(LiteralBooleanExpression.TRUE, "col2")
                            ), null),
                            "",
                            "object",
                            null),
                    asList(new AliasExpression(intLit(1), "col3"), new AliasExpression(uce("__expr0"), "values")));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        //@formatter:off
        Assertions.assertThat(actual.getSchema())
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(Schema.of(col("col3", Type.Int),
                        col("values", ResolvedType.ANY)));

        // Validate the schema below the projection. The schema must stay consistent even if we switch outer/inner
        Assertions.assertThat(((Projection) actual).getInput()
                .getSchema())
            .usingRecursiveComparison()
            .ignoringFieldsOfTypes(Location.class, Random.class)
            .isEqualTo(Schema.of(
                    col("__expr0", ResolvedType.ANY, null, true)));
        //@formatter:on

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
                + ") \"values\" "
                + "from \"table\" ";

        ILogicalPlan plan = getSchemaResolvedPlan(query);

        ILogicalPlan actual = optimize(context, plan);
        
        TableSourceReference table = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("table"), "");
        TableSourceReference tableB = new TableSourceReference(1, TableSourceReference.Type.TABLE, "", QualifiedName.of("tableB"), "b");
        Schema schema = Schema.of(ast("", table));
        Schema schemaB = Schema.of(ast("b", tableB));

        //@formatter:off
        ILogicalPlan expected = 
                projection(
                    new Join(
                        tableScan(schema, table),
                        new MaxRowCountAssert(
                                projection(
                                    tableScan(schemaB, tableB),
                                asList(new AliasExpression(uce("b", "col1"), "__expr0", true))),
                            1),
                        Join.Type.LEFT,
                        null,
                        (IExpression) null,
                        asSet(),
                        false,
                        Schema.EMPTY),
                    asList(new AsteriskExpression(QualifiedName.EMPTY, null, Set.of()), new AliasExpression(uce("__expr0"), "values")));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        //@formatter:off
        Assertions.assertThat(actual.getSchema())
            .usingRecursiveComparison()
            .ignoringFieldsOfTypes(Location.class, Random.class)
            .isEqualTo(Schema.of(
                ast("", "*", null),
                col("values", ResolvedType.ANY)));

        // Validate the schema below the projection. The schema must stay consistent even if we switch outer/inner
        Assertions.assertThat(((Projection) actual).getInput()
                .getSchema())
            .usingRecursiveComparison()
            .ignoringFieldsOfTypes(Location.class, Random.class)
            .isEqualTo(Schema.of(
                    ast("", table),
                    col("__expr0", ResolvedType.ANY, null, true)));
        //@formatter:on

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_aggregation_without_for()
    {
        //@formatter:off
        String query = """
                select *,
                (
                  select b.col1 MyCol
                  from tableB b
                  group by b.col1
                ) \"values\",
                (
                  select max(b.col1) MyCol2
                  from tableB b
                  group by b.col1
                ) values2
                from \"table\"
                """;
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);

        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference table = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("table"), "");
        TableSourceReference tableB = new TableSourceReference(1, TableSourceReference.Type.TABLE, "", QualifiedName.of("tableB"), "b");
        TableSourceReference tableB2 = new TableSourceReference(2, TableSourceReference.Type.TABLE, "", QualifiedName.of("tableB"), "b");
        Schema schema = Schema.of(ast("", table));
        Schema schemaB = Schema.of(ast("b", tableB));
        Schema schemaB2 = Schema.of(ast("b", tableB2));

        ScalarFunctionInfo max = SystemCatalog.get()
                .getScalarFunction("max");

        //@formatter:off
        ILogicalPlan expected = 
                projection(
                    new Join(
                        new Join(
                            tableScan(schema, table),
                            new MaxRowCountAssert(
                                new Aggregate(
                                    tableScan(schemaB, tableB),
                                    List.of(uce("b", "col1")),
                                    List.of(new AggregateWrapperExpression(new AliasExpression(uce("b", "col1"), "__expr0", true), false, true)),
                                    null),
                                1
                            ),
                            Join.Type.LEFT,
                            null,
                            (IExpression) null,
                            Set.of(),
                            false,
                            Schema.EMPTY),
                        new MaxRowCountAssert(
                            new Aggregate(
                                tableScan(schemaB2, tableB2),
                                List.of(uce("b", "col1")),
                                List.of(new AggregateWrapperExpression(new AliasExpression(
                                        new FunctionCallExpression("sys", max, null, List.of(uce("b", "col1"))), "__expr1", true),
                                        false, true)),
                                null),
                                        //(new AliasExpression(uce("b", "col1"), "__expr1", true), false, true))),
                            1
                        ),
                        Join.Type.LEFT,
                        null,
                        (IExpression) null,
                        asSet(),
                        false,
                        Schema.EMPTY),
                    asList(
                        new AsteriskExpression(QualifiedName.EMPTY, null, Set.of()),
                        new AliasExpression(uce("__expr0"), "values"),
                        new AliasExpression(uce("__expr1"), "values2")
                    ));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        //@formatter:off
        Assertions.assertThat(actual.getSchema())
            .usingRecursiveComparison()
            .ignoringFieldsOfTypes(Location.class, Random.class)
            .isEqualTo(Schema.of(
                ast("", "*", null),
                col("values", ResolvedType.ANY),
                col("values2", ResolvedType.ANY)));

        // Validate the schema below the projection. The schema must stay consistent even if we switch outer/inner
        Assertions.assertThat(((Projection) actual).getInput()
                .getSchema())
            .usingRecursiveComparison()
            .ignoringFieldsOfTypes(Location.class, Random.class)
            .isEqualTo(Schema.of(
                    ast("", table),
                    // NOTE! Single value is resolved at a later stage (ColumnResolver)
                    col("__expr0", ResolvedType.array(ResolvedType.ANY), null, true),
                    col("__expr1", ResolvedType.ANY, null, true)));
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
                + ") \"values\", "
                + "( "
                + "  select c.col2 "
                + "  from tableC c "
                + ") otherValues "
                + "from \"table\" ";

        ILogicalPlan plan = getSchemaResolvedPlan(query);

        ILogicalPlan actual = optimize(context, plan);
        
        TableSourceReference table = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("table"), "");
        TableSourceReference tableB = new TableSourceReference(1, TableSourceReference.Type.TABLE, "", QualifiedName.of("tableB"), "b");
        TableSourceReference tableC = new TableSourceReference(2, TableSourceReference.Type.TABLE, "", QualifiedName.of("tableC"), "c");
        Schema schema = Schema.of(ast("", table));
        Schema schemaB = Schema.of(ast("b", tableB));
        Schema schemaC = Schema.of(ast("c", tableC));

        //@formatter:off
        ILogicalPlan expected = 
                projection(
                    new Join(
                        new Join(
                            tableScan(schema, table),
                            new MaxRowCountAssert(
                                projection(
                                        tableScan(schemaB, tableB),
                                        asList(new AliasExpression(uce("b", "col1"), "__expr0", true))),
                                1),
                            Join.Type.LEFT,
                            null,
                            (IExpression) null,
                            asSet(),
                            false,
                            Schema.EMPTY),
                        new MaxRowCountAssert(
                            projection(
                                    tableScan(schemaC, tableC),
                                    asList(new AliasExpression(uce("c", "col2"), "__expr1", true))),
                            1),
                        Join.Type.LEFT,
                        null,
                        (IExpression) null,
                        asSet(),
                        false,
                        Schema.EMPTY),
                    asList(new AsteriskExpression(QualifiedName.EMPTY, null, Set.of()),
                            new AliasExpression(uce("__expr0"), "values"),
                            new AliasExpression(uce("__expr1"), "otherValues")));
        //@formatter:on

        //@formatter:off
        Assertions.assertThat(actual.getSchema())
            .usingRecursiveComparison()
            .ignoringFieldsOfTypes(Location.class, Random.class)
            .isEqualTo(Schema.of(
                ast("", "*", null),
                col("values", Type.Any),
                col("otherValues", Type.Any)));

        // Validate the schema below the projection
        Assertions.assertThat( ((Projection) actual).getInput()
                .getSchema())
        .usingRecursiveComparison()
        .ignoringFieldsOfTypes(Location.class, Random.class)
        .isEqualTo(Schema.of(
                ast("", table),
                col("__expr0", ResolvedType.ANY, null, true),
                col("__expr1", ResolvedType.ANY, null, true)));
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
                + ") \"values\", "
                + "( "
                + "  select c.col2 "
                + "  from tableC c "
                + ") otherValues "
                + "from \"table\" a ";

        ILogicalPlan plan = getSchemaResolvedPlan(query);

        ILogicalPlan actual = optimize(context, plan);
        
        TableSourceReference table = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("table"), "a");
        TableSourceReference tableB = new TableSourceReference(1, TableSourceReference.Type.TABLE, "", QualifiedName.of("tableB"), "b");
        TableSourceReference tableC = new TableSourceReference(2, TableSourceReference.Type.TABLE, "", QualifiedName.of("tableC"), "c");
        Schema schema = Schema.of(ast("a", table));
        Schema schemaB = Schema.of(ast("b", tableB));
        Schema schemaC = Schema.of(ast("c", tableC));

        //@formatter:off
        ILogicalPlan expected = 
                projection(
                    new Join(
                        new Join(
                            tableScan(schema, table),
                            new MaxRowCountAssert(
                                projection(
                                    new Filter(
                                        tableScan(schemaB, tableB),
                                        null,
                                        eq(uce("b", "col2"), uce("a", "col2"))),
                                    asList(new AliasExpression(uce("b", "col1"), "__expr0", true))),
                            1),
                            Join.Type.LEFT,
                            null,
                            (IExpression) null,
                            Set.of(),
                            false,
                            Schema.EMPTY),
                        new MaxRowCountAssert(
                            projection(
                                tableScan(schemaC, tableC),
                                asList(new AliasExpression(uce("c", "col2"), "__expr1", true))),
                            1),
                        Join.Type.LEFT,
                        null,
                        (IExpression) null,
                        asSet(),
                        false,
                        Schema.EMPTY),
                    asList(new AsteriskExpression(QualifiedName.EMPTY, null, Set.of()),
                            new AliasExpression(uce("__expr0"), "values"),
                            new AliasExpression(uce("__expr1"), "otherValues")));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual.getSchema())
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(Schema.of(ast("", "*", null), col("values", Type.Any), col("otherValues", Type.Any)));

        // Validate the schema below the projection. The schema must stay consistent even if we switch outer/inner
        Assertions.assertThat(((Projection) actual).getInput()
                .getSchema())
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(Schema.of(ast("a", table), col("__expr0", ResolvedType.ANY, null, true), col("__expr1", ResolvedType.ANY, null, true)));
        //@formatter:on

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
                + ") \"values\" "
                + "from \"table\" a ";

        ILogicalPlan plan = getSchemaResolvedPlan(query);

        ILogicalPlan actual = optimize(context, plan);
        
        TableSourceReference table = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("table"), "a");
        TableSourceReference tableB = new TableSourceReference(2, TableSourceReference.Type.TABLE, "", QualifiedName.of("tableB"), "b");
        TableSourceReference tableC = new TableSourceReference(1, TableSourceReference.Type.TABLE, "", QualifiedName.of("tableC"), "c");
        Schema schema = Schema.of(ast("a", table));
        Schema schemaB = Schema.of(ast("b", tableB));
        Schema schemaC = Schema.of(ast("c", tableC));

        //@formatter:off
        ILogicalPlan expected = 
                projection(
                    new Join(
                        new Join(
                            tableScan(schema, table),
                            new MaxRowCountAssert(
                                projection(
                                        tableScan(schemaC, tableC),
                                        asList(new AliasExpression(uce("c", "col2"), "__expr0", true))),
                                1),
                            Join.Type.LEFT,
                            null,
                            (IExpression) null,
                            asSet(),
                            false,
                            Schema.EMPTY),
                        new MaxRowCountAssert(
                            projection(
                                new Filter(
                                    tableScan(schemaB, tableB),
                                    null,
                                    eq(uce("b", "col2"), uce("a", "col2"))),
                                asList(new AliasExpression(uce("b", "col1"), "__expr1", true))),
                        1),
                        Join.Type.LEFT,
                        null,
                        (IExpression) null,
                        Set.of(),
                        false,
                        Schema.EMPTY),
                    asList(new AsteriskExpression(QualifiedName.EMPTY, null, Set.of()),
                            new AliasExpression(uce("__expr0"), "otherValues"),
                            new AliasExpression(uce("__expr1"), "values")));
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
                + ") \"values\" "
                + "from \"table\" a ";

        ILogicalPlan plan = getSchemaResolvedPlan(query);

        ILogicalPlan actual = optimize(context, plan);
        
        TableSourceReference table = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("table"), "a");
        TableSourceReference tableB = new TableSourceReference(2, TableSourceReference.Type.TABLE, "", QualifiedName.of("tableB"), "b");
        TableSourceReference tableC = new TableSourceReference(1, TableSourceReference.Type.TABLE, "", QualifiedName.of("tableC"), "c");
        Schema schema = Schema.of(ast("a", table));
        Schema schemaB = Schema.of(ast("b", tableB));
        Schema schemaC = Schema.of(ast("c", tableC));

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
                                        eq(uce("c", "col3"), uce("a", "col3"))),
                                    asList(new AliasExpression(uce("c", "col2"), "__expr0", true))),
                            1),
                            Join.Type.LEFT,
                            null,
                            (IExpression) null,
                            Set.of(),
                            false,
                            Schema.EMPTY),
                        new MaxRowCountAssert(
                            projection(
                                new Filter(
                                    tableScan(schemaB, tableB),
                                    null,
                                    eq(uce("b", "col2"), uce("a", "col2"))),
                                asList(new AliasExpression(uce("b", "col1"), "__expr1", true))),
                        1),
                        Join.Type.LEFT,
                        null,
                        (IExpression) null,
                        Set.of(),
                        false,
                        Schema.EMPTY),
                    asList(new AsteriskExpression(QualifiedName.EMPTY, null, Set.of()),
                            new AliasExpression(uce("__expr0"), "otherValues"),
                            new AliasExpression(uce("__expr1"), "values")));
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
                + ") + 1 \"values\" "
                + "from \"table\" ";

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);
        
        TableSourceReference table = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("table"), "");
        TableSourceReference tableB = new TableSourceReference(1, TableSourceReference.Type.TABLE, "", QualifiedName.of("tableB"), "b");
        Schema schema = Schema.of(ast("", table));
        Schema schemaB = Schema.of(ast("b", tableB));
        
        //@formatter:off
        ILogicalPlan expected = 
                projection(
                    new Join(
                        tableScan(schema, table),
                        new MaxRowCountAssert(
                            projection(
                                    tableScan(schemaB, tableB),
                                    asList(new AliasExpression(uce("b", "col1"), "__expr0", true))),
                            1),
                        Join.Type.LEFT,
                        null,
                        (IExpression) null,
                        asSet(),
                        false,
                        Schema.EMPTY),
                    asList(new AsteriskExpression(QualifiedName.EMPTY, null, Set.of()),
                            new AliasExpression(add(uce("__expr0"), intLit(1)), "values")));
        //@formatter:on

        // System.out.println(actual.print(0));
        // System.out.println(expected.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

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
                + ") \"values\" "
                + "from \"table\" ";

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);
        
        TableSourceReference table = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("table"), "");
        TableSourceReference tableB = new TableSourceReference(1, TableSourceReference.Type.TABLE, "", QualifiedName.of("tableB"), "b");
        Schema schema = Schema.of(ast("", table));
        Schema schemaB = Schema.of(ast("b", tableB));
        
        //@formatter:off
        ILogicalPlan expected = 
                projection(
                    new Join(
                        tableScan(schema, table),
                        new MaxRowCountAssert(
                                projection(
                                    tableScan(schemaB, tableB),
                                    List.of(new AliasExpression(uce("b", "col1"), "__expr0", true))),
                                1),
                        Join.Type.LEFT,
                        null,
                        (IExpression) null,
                        asSet(),
                        false,
                        Schema.EMPTY),
                    asList(new AsteriskExpression(QualifiedName.EMPTY, null, Set.of()),
                            new AliasExpression(add(uce("__expr0"), intLit(1)), "values")));
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
                + ") \"values\" "
                + "from \"table\" a ";

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);
        
        TableSourceReference table = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("table"), "a");
        TableSourceReference tableB = new TableSourceReference(1, TableSourceReference.Type.TABLE, "", QualifiedName.of("tableB"), "b");
        Schema schema = Schema.of(ast("a", table));
        Schema schemaB = Schema.of(ast("b", tableB));
        
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
                                    gt(uce("b", "col2"), uce("a", "col2"))),
                                asList(new AliasExpression(uce("b", "col1"), "__expr0", true))),
                        1),
                        Join.Type.LEFT,
                        null,
                        (IExpression) null,
                        Set.of(),
                        false,
                        Schema.EMPTY),
                    asList(new AsteriskExpression(QualifiedName.EMPTY, null, Set.of()), new AliasExpression(uce("__expr0"), "values")));
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
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference table = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("table"), "a");
        TableSourceReference tableB = new TableSourceReference(1, TableSourceReference.Type.TABLE, "", QualifiedName.of("tableB"), "b");
        TableSourceReference tableC = new TableSourceReference(2, TableSourceReference.Type.TABLE, "", QualifiedName.of("tableC"), "c");
        Schema schema = Schema.of(ast("a", table));
        Schema schemaB = Schema.of(ast("b", tableB));
        Schema schemaC = Schema.of(ast("c", tableC));

        //@formatter:off
        ILogicalPlan expected = 
                projection(
                    new Join(
                        new Join(
                            tableScan(schema, table),
                            new MaxRowCountAssert(
                                projection(                             // Correlated sub query then it needs to be inner
                                    new Filter(
                                        tableScan(schemaB, tableB),
                                        null,
                                        gt(uce("b", "col2"), uce("a", "col2"))),
                                    asList(new AliasExpression(uce("b", "col1"), "__expr0", true))),
                                1),
                            Join.Type.LEFT,
                            null,
                            (IExpression) null,
                            Set.of(),
                            false,
                            Schema.EMPTY),
                        new MaxRowCountAssert(
                            projection(                                 // Non correlated sub query is placed outer
                                tableScan(schemaC, tableC), 
                                asList(new AliasExpression(uce("c", "col2"), "__expr1", true))),
                            1),
                        Join.Type.LEFT,
                        null,
                        (IExpression) null,
                        asSet(),
                        false,
                        Schema.EMPTY),
                    asList(new AsteriskExpression(QualifiedName.EMPTY, null, Set.of()),
                            new AliasExpression(
                            add(uce("__expr0"), uce("__expr1")), "value")));
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
        ALogicalPlanOptimizer.Context ctx = rule.createContext(context);
        return rule.optimize(ctx, plan);
    }
}
