package se.kuseman.payloadbuilder.core.logicalplan.optimization;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.core.utils.CollectionUtils.asSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ISortItem;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.catalog.system.SystemCatalog;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;
import se.kuseman.payloadbuilder.core.expression.AliasExpression;
import se.kuseman.payloadbuilder.core.expression.AsteriskExpression;
import se.kuseman.payloadbuilder.core.expression.FunctionCallExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralBooleanExpression;
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

        plan = LogicalPlanOptimizer.optimize(context, plan, new HashMap<>(), new HashMap<>());
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference pdArticle = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("PDArticle"), "a");
        TableSourceReference a_resource = new TableSourceReference(1, TableSourceReference.Type.EXPRESSION, "", QualifiedName.of("a.resource"), "ar");

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
                                            ocre("resource", pdArticle, ResolvedType.of(Type.Any), CoreColumn.Type.NAMED_ASTERISK),
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
                                                    asList(cre("defaultfront", a_resource, ResolvedType.of(Type.Any)), intLit(0))), ISortItem.Order.ASC))
                            ),
                            intLit(1)),
                        Join.Type.LEFT,
                        null,
                        (IExpression) null,
                        asSet(nast("resource", ResolvedType.of(Type.Any), pdArticle)),
                        false,
                        schemaPDArticle),
                    asList(new AliasExpression(cre("__expr0", a_resource, ResolvedType.of(Type.Any)), "images")));
        //@formatter:on

        //@formatter:off
        Assertions.assertThat(actual.getSchema())
            .usingRecursiveComparison()
            .ignoringFieldsOfTypes(Location.class, Random.class)
            .isEqualTo(Schema.of(
                    nast("images", ResolvedType.of(Type.Any), a_resource)));
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

        ILogicalPlan plan = getColumnResolvedPlan(q);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference tableProduct = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("product"), "p");
        TableSourceReference tableCategory = new TableSourceReference(1, TableSourceReference.Type.TABLE, "", QualifiedName.of("category"), "c");
        TableSourceReference t = new TableSourceReference(2, TableSourceReference.Type.EXPRESSION, "", QualifiedName.of("c.tbl"), "t");
        TableSourceReference t2 = new TableSourceReference(3, TableSourceReference.Type.EXPRESSION, "", QualifiedName.of("t.tbl2"), "t2");

        Schema schemaProduct = Schema.of(ast("p", Type.Any, tableProduct));
        Schema schemaCategory = Schema.of(ast("c", Type.Any, tableCategory));

        Schema schemaT = Schema.of(ast("t", Type.Any, t));
        Schema schemaT2 = Schema.of(ast("t2", Type.Any, t2));

        //@formatter:off
        Schema categorySchema = Schema.of(
                nast("name", Type.Any, tableCategory),
                nast("id", Type.Any, tableCategory),
                col("fields", ResolvedType.table(Schema.of(
                        new CoreColumn("", ResolvedType.ANY, "t2.*", false, t2, CoreColumn.Type.ASTERISK))), null)
                );

        ILogicalPlan expected = projection(
                new Join(
                    new Join(
                        tableScan(schemaProduct, tableProduct),
                        tableScan(schemaCategory, tableCategory),
                        Join.Type.INNER,
                        null,
                        eq(cre("id", tableCategory), cre("categoryId", tableProduct)),
                        Set.of(),
                        false,
                        Schema.EMPTY),
                    new OperatorFunctionScan(
                        Schema.of(nast("__expr0", ResolvedType.object(categorySchema), null)),
                        projection(
                            new Join(
                                ConstantScan.INSTANCE,
                                new OperatorFunctionScan(
                                    Schema.of(nast("__expr1", categorySchema.getColumns().get(2).getType(), null)),
                                    projection(
                                        new Filter(
                                            new Join(
                                                new ExpressionScan(t, schemaT, ocre("tbl", tableCategory), null),
                                                new ExpressionScan(t2, schemaT2, ocre("tbl2", t), null),
                                                Join.Type.INNER,
                                                null,
                                                null,
                                                Set.of(nast("tbl2", Type.Any, t)),
                                                false,
                                                Schema.of(
                                                    ast("p", Type.Any, tableProduct),
                                                    ast("c", Type.Any, tableCategory),
                                                    ast("t", Type.Any, t)
                                                )),
                                            null,
                                            eq(cre("languagecode", t2), new LiteralStringExpression("sv"))),
                                        List.of(new AsteriskExpression(QualifiedName.of("t2"), null, Set.of(t2)))),
                                    "",
                                    "OBJECT_ARRAY",
                                    null),
                                Join.Type.LEFT,
                                null,
                                null,
                                Set.of(nast("tbl2", Type.Any, t), nast("tbl", Type.Any, tableCategory)),
                                false,
                                Schema.of(
                                    ast("p", Type.Any, tableProduct),
                                    ast("c", Type.Any, tableCategory)
                                )),
                            List.of(ocre("name", tableCategory),
                                    ocre("id", tableCategory),
                                    new AliasExpression(ce("__expr1", 0, categorySchema.getColumns().get(2).getType()), "fields"))),
                        "",
                        "OBJECT",
                        null),
                    Join.Type.LEFT,
                    null,
                    null,
                    Set.of(
                        nast("tbl2", Type.Any, t),
                        nast("name", Type.Any, tableCategory),
                        nast("id", Type.Any, tableCategory),
                        nast("tbl", Type.Any, tableCategory)
                    ),
                    false,
                    Schema.of(
                        ast("p", Type.Any, tableProduct),
                        ast("c", Type.Any, tableCategory)
                    )),
                List.of(new AliasExpression(ce("__expr0", ResolvedType.object(categorySchema)), "category")));
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
                     )
                   ) values
                   from PDProduct_Article a
                   cross populate apply tableB bb
                """;

        ILogicalPlan plan = getColumnResolvedPlan(q);

        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference tableProductArticle = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("PDProduct_Article"), "a");
        TableSourceReference tableArticle = new TableSourceReference(3, TableSourceReference.Type.TABLE, "", QualifiedName.of("PDArticle"), "a");
        TableSourceReference tableProduct = new TableSourceReference(4, TableSourceReference.Type.TABLE, "", QualifiedName.of("PDProduct"), "p");
        TableSourceReference tableB = new TableSourceReference(1, TableSourceReference.Type.TABLE, "", QualifiedName.of("tableB"), "bb");
        TableSourceReference e_bb = new TableSourceReference(2, TableSourceReference.Type.EXPRESSION, "", QualifiedName.of("bb"), "bb");

        Schema schemaProductArticle = Schema.of(ast("a", Type.Any, tableProductArticle));
        Schema schemaArticle = Schema.of(ast("a", Type.Any, tableArticle));
        Schema schemaProduct = Schema.of(ast("p", Type.Any, tableProduct));
        Schema schemaTableB = Schema.of(ast("bb", Type.Any, tableB));
        Schema schemaBB = Schema.of(ast("bb", ResolvedType.of(Type.Any), e_bb));

        //@formatter:off
        Schema objectArraySchema = Schema.of(
                col("val", ResolvedType.of(Type.Any), null),
                col("val1", ResolvedType.of(Type.Any), null),
                nast("col5", ResolvedType.of(Type.Any), tableProductArticle));

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
                            schemaProductArticle),
                        projection(
                            new Join(
                                ConstantScan.INSTANCE,
                                new OperatorFunctionScan(Schema.of(nast("__expr1", ResolvedType.table(objectArraySchema), null)),
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
                                                   false,
                                                   Schema.EMPTY),
                                               new ExpressionScan(
                                                   e_bb,
                                                   schemaBB,
                                                   ocre("bb", tableB, ResolvedType.table(schemaTableB), CoreColumn.Type.POPULATED),
                                                   null),
                                               Join.Type.LEFT,
                                               null,
                                               (IExpression) null,
                                               asSet(),
                                               true,
                                               SchemaUtils.joinSchema(SchemaUtils.joinSchema(schemaProductArticle, schemaTableB, "bb"), schemaBB)),
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
                                           asSet(nast("col", Type.Any, tableProductArticle)),
                                           false,
                                           SchemaUtils.joinSchema(
                                               SchemaUtils.joinSchema(
                                                   SchemaUtils.joinSchema(schemaProductArticle, schemaTableB, "bb"), schemaBB),
                                                   Schema.of(nast("__expr2", ResolvedType.of(Type.Any), tableArticle)))),
                                       asList(new AliasExpression(cre("__expr2", tableArticle), "val"),
                                               new AliasExpression(cre("__expr3", tableProduct), "val1"), ocre("col5", tableProductArticle))
                                    ),
                                    "",
                                    "object_array",
                                    null),
                                Join.Type.LEFT,
                                null,
                                (IExpression) null,
                                asSet(pop("bb", ResolvedType.table(schemaTableB), tableB), nast("col", Type.Any, tableProductArticle), nast("col5", Type.Any, tableProductArticle)),
                                false,
                                SchemaUtils.joinSchema(schemaProductArticle, schemaTableB, "bb")),
                            asList(new AliasExpression(ce("__expr1", 0, ResolvedType.table(objectArraySchema)), "__expr0"))),
                        Join.Type.LEFT,
                        null,
                        (IExpression) null,
                        asSet(pop("bb", ResolvedType.table(schemaTableB), tableB), nast("col", Type.Any, tableProductArticle), nast("col5", Type.Any, tableProductArticle)),
                        false,
                        SchemaUtils.joinSchema(schemaProductArticle, schemaTableB, "bb")),
                    asList(cre("column", tableProductArticle),
                            new AliasExpression(ce("__expr0", ResolvedType.table(objectArraySchema)), "values")));
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
                + ") values "
                + "from \"stableA\" a";
        //@formatter:on
        ILogicalPlan plan = getColumnResolvedPlan(query);

        ILogicalPlan actual = optimize(context, plan);

        Schema objectSchema = Schema.of(CoreColumn.of("col1", ResolvedType.of(Type.Int), null), CoreColumn.of("col2", ResolvedType.of(Type.Boolean), null));

        //@formatter:off
        ILogicalPlan expected =
                projection(
                    new Join(
                        new Join(
                            ConstantScan.INSTANCE,
                            new OperatorFunctionScan(
                                    Schema.of(col("__expr0", ResolvedType.object(objectSchema), null)),
                                    projection(
                                        ConstantScan.INSTANCE,
                                        asList(new AliasExpression(intLit(1), "col1"), new AliasExpression(LiteralBooleanExpression.TRUE, "col2"))),
                                    "",
                                    "object",
                                    null),
                            Join.Type.LEFT,
                            null,
                            (IExpression) null,
                            asSet(),
                            false,
                            Schema.EMPTY),
                        tableScan(schemaSTableA, sTableA),
                        Join.Type.LEFT,
                        null,
                        (IExpression) null,
                        asSet(),
                        true,
                        schemaSTableA),
                    asList(cre("col3", sTableA, 2, ResolvedType.of(Type.Float)), new AliasExpression(ce("__expr0", 3,
                            ResolvedType.object(objectSchema)), "values")));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        //@formatter:off
        Assertions.assertThat(actual.getSchema())
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(Schema.of(col("col3", Type.Float, sTableA),
                        new CoreColumn("values", ResolvedType.object(objectSchema), "",
                                false, null, CoreColumn.Type.REGULAR)));

        // Validate the schema below the projection. The schema must stay consistent even if we switch outer/inner
        Assertions.assertThat(((Projection) actual).getInput()
                .getSchema())
            .usingRecursiveComparison()
            .ignoringFieldsOfTypes(Location.class, Random.class)
            .isEqualTo(Schema.of(
                    col("col1", Type.Int, sTableA),
                    col("col2", Type.String, sTableA),
                    col("col3", Type.Float, sTableA),
                    col("__expr0", ResolvedType.object(objectSchema), null)));
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
                + ") values ";
        //@formatter:on
        ILogicalPlan plan = getColumnResolvedPlan(query);

        ILogicalPlan actual = optimize(context, plan);

        Schema objectSchema = Schema.of(CoreColumn.of("col1", ResolvedType.of(Type.Int), null), CoreColumn.of("col2", ResolvedType.of(Type.Boolean), null));

        //@formatter:off
        ILogicalPlan expected =
                projection(
                    new OperatorFunctionScan(
                            Schema.of(col("__expr0", ResolvedType.object(objectSchema), null)),
                            projection(
                                ConstantScan.INSTANCE,
                                asList(new AliasExpression(intLit(1), "col1"), new AliasExpression(LiteralBooleanExpression.TRUE, "col2"))),
                            "",
                            "object",
                            null),
                    asList(new AliasExpression(intLit(1), "col3"), new AliasExpression(ce("__expr0", 0,
                            ResolvedType.object(objectSchema)), "values")));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        //@formatter:off
        Assertions.assertThat(actual.getSchema())
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(Schema.of(col("col3", Type.Int, null),
                        new CoreColumn("values", ResolvedType.object(objectSchema), "",
                                false, null, CoreColumn.Type.REGULAR)));

        // Validate the schema below the projection. The schema must stay consistent even if we switch outer/inner
        Assertions.assertThat(((Projection) actual).getInput()
                .getSchema())
            .usingRecursiveComparison()
            .ignoringFieldsOfTypes(Location.class, Random.class)
            .isEqualTo(Schema.of(
                    col("__expr0", ResolvedType.object(objectSchema), null)));
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
                + ") values "
                + "from \"table\" ";

        ILogicalPlan plan = getColumnResolvedPlan(query);

        ILogicalPlan actual = optimize(context, plan);
        
        TableSourceReference table = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("table"), "");
        TableSourceReference tableB = new TableSourceReference(1, TableSourceReference.Type.TABLE, "", QualifiedName.of("tableB"), "b");
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
                            false,
                            Schema.EMPTY),
                        tableScan(schema, table),
                        Join.Type.LEFT,
                        null,
                        (IExpression) null,
                        asSet(),
                        true,
                        schema),
                    asList(new AsteriskExpression(QualifiedName.EMPTY, null, Set.of(table)), new AliasExpression(cre("__expr0", tableB), "values")));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        //@formatter:off
        Assertions.assertThat(actual.getSchema())
            .usingRecursiveComparison()
            .ignoringFieldsOfTypes(Location.class, Random.class)
            .isEqualTo(Schema.of(
                new CoreColumn("", ResolvedType.of(Type.Any), "*", false, table, CoreColumn.Type.ASTERISK),
                new CoreColumn("values", ResolvedType.of(Type.Any), "", false, tableB, CoreColumn.Type.NAMED_ASTERISK)));

        // Validate the schema below the projection. The schema must stay consistent even if we switch outer/inner
        Assertions.assertThat(((Projection) actual).getInput()
                .getSchema())
            .usingRecursiveComparison()
            .ignoringFieldsOfTypes(Location.class, Random.class)
            .isEqualTo(Schema.of(
                    new CoreColumn("", ResolvedType.of(Type.Any), "", false, table, CoreColumn.Type.ASTERISK),
                    nast("__expr0", Type.Any, tableB)));
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
        
        TableSourceReference table = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("table"), "");
        TableSourceReference tableB = new TableSourceReference(1, TableSourceReference.Type.TABLE, "", QualifiedName.of("tableB"), "b");
        TableSourceReference tableC = new TableSourceReference(2, TableSourceReference.Type.TABLE, "", QualifiedName.of("tableC"), "c");
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
                            false,
                            Schema.EMPTY),
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
                                false,
                                Schema.EMPTY),
                            tableScan(schema, table),
                            Join.Type.LEFT,
                            null,
                            (IExpression) null,
                            asSet(),
                            true,
                            schema),
                        Join.Type.LEFT,
                        null,
                        (IExpression) null,
                        asSet(),
                        true,
                        SchemaUtils.joinSchema(schema, Schema.of(nast("__expr0", ResolvedType.of(Type.Any), tableB)))),
                    asList(new AsteriskExpression(QualifiedName.EMPTY, null, Set.of(table)),
                            new AliasExpression(cre("__expr0", tableB), "values"),
                            new AliasExpression(cre("__expr1", tableC), "otherValues")));
        //@formatter:on

        //@formatter:off
        Assertions.assertThat(actual.getSchema())
            .usingRecursiveComparison()
            .ignoringFieldsOfTypes(Location.class, Random.class)
            .isEqualTo(Schema.of(
                new CoreColumn("", ResolvedType.of(Type.Any), "*", false, table, CoreColumn.Type.ASTERISK),
                nast("values", Type.Any, tableB),
                nast("otherValues", Type.Any, tableC)));

        // Validate the schema below the projection
        Assertions.assertThat( ((Projection) actual).getInput()
                .getSchema())
        .usingRecursiveComparison()
        .ignoringFieldsOfTypes(Location.class, Random.class)
        .isEqualTo(Schema.of(
                new CoreColumn("", ResolvedType.of(Type.Any), "", false, table, CoreColumn.Type.ASTERISK),
                nast("__expr0", Type.Any, tableB),
                nast("__expr1", Type.Any, tableC)));
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
        
        TableSourceReference table = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("table"), "a");
        TableSourceReference tableB = new TableSourceReference(1, TableSourceReference.Type.TABLE, "", QualifiedName.of("tableB"), "b");
        TableSourceReference tableC = new TableSourceReference(2, TableSourceReference.Type.TABLE, "", QualifiedName.of("tableC"), "c");
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
                            false,
                            Schema.EMPTY),
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
                            asSet(nast("col2", Type.Any, table)),
                            false,
                            schema),
                        Join.Type.LEFT,
                        null,
                        (IExpression) null,
                        asSet(),
                        true,
                        SchemaUtils.joinSchema(schema, Schema.of(nast("__expr0", ResolvedType.of(Type.Any), tableB)))),
                    asList(new AsteriskExpression(QualifiedName.EMPTY, null, Set.of(table)),
                            new AliasExpression(cre("__expr0", tableB), "values"),
                            new AliasExpression(cre("__expr1", tableC), "otherValues") ) );

        //@formatter:off
        Assertions.assertThat(actual.getSchema())
            .usingRecursiveComparison()
            .ignoringFieldsOfTypes(Location.class, Random.class)
            .isEqualTo(Schema.of(
                new CoreColumn("", ResolvedType.of(Type.Any), "*", false, table, CoreColumn.Type.ASTERISK),
                nast("values", Type.Any, tableB),
                nast("otherValues", Type.Any, tableC)));

        // Validate the schema below the projection. The schema must stay consistent even if we switch outer/inner
        Assertions.assertThat( ((Projection) actual).getInput()
                .getSchema())
        .usingRecursiveComparison()
        .ignoringFieldsOfTypes(Location.class, Random.class)
        .isEqualTo(Schema.of(
                new CoreColumn("a", ResolvedType.of(Type.Any), "a", false, table, CoreColumn.Type.ASTERISK),
                nast("__expr0", Type.Any, tableB),
                nast("__expr1", Type.Any, tableC)));
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
        
        TableSourceReference table = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("table"), "a");
        TableSourceReference tableB = new TableSourceReference(2, TableSourceReference.Type.TABLE, "", QualifiedName.of("tableB"), "b");
        TableSourceReference tableC = new TableSourceReference(1, TableSourceReference.Type.TABLE, "", QualifiedName.of("tableC"), "c");
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
                                false,
                                Schema.EMPTY),
                            tableScan(schema, table),
                            Join.Type.LEFT,
                            null,
                            (IExpression) null,
                            asSet(),
                            true,
                            schema),
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
                        asSet(nast("col2", Type.Any, table)),
                        false,
                        SchemaUtils.joinSchema(schema, Schema.of(nast("__expr0", ResolvedType.of(Type.Any), tableC)))),
                    asList(new AsteriskExpression(QualifiedName.EMPTY, null, Set.of(table)),
                            new AliasExpression(cre("__expr0", tableC), "otherValues"),
                            new AliasExpression(cre("__expr1", tableB), "values")));
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
        
        TableSourceReference table = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("table"), "a");
        TableSourceReference tableB = new TableSourceReference(2, TableSourceReference.Type.TABLE, "", QualifiedName.of("tableB"), "b");
        TableSourceReference tableC = new TableSourceReference(1, TableSourceReference.Type.TABLE, "", QualifiedName.of("tableC"), "c");
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
                            asSet(nast("col3", Type.Any, table)),
                            false,
                            schema),
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
                        asSet(nast("col2", Type.Any, table)),
                        false,
                        SchemaUtils.joinSchema(schema, Schema.of(nast("__expr0", ResolvedType.of(Type.Any), tableC)))),
                    asList(new AsteriskExpression(QualifiedName.EMPTY, null, Set.of(table)),
                            new AliasExpression(cre("__expr0", tableC), "otherValues"),
                            new AliasExpression(cre("__expr1", tableB), "values")));
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
        
        TableSourceReference table = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("table"), "");
        TableSourceReference tableB = new TableSourceReference(1, TableSourceReference.Type.TABLE, "", QualifiedName.of("tableB"), "b");
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
                            false,
                            Schema.EMPTY),
                        tableScan(schema, table),
                        Join.Type.LEFT,
                        null,
                        (IExpression) null,
                        asSet(),
                        true,
                        schema),
                    asList(new AsteriskExpression(QualifiedName.EMPTY, null, Set.of(table)),
                            new AliasExpression(add(cre("__expr0", tableB), intLit(1)), "values")));
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
                + ") values "
                + "from \"table\" ";

        ILogicalPlan plan = getColumnResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);
        
        TableSourceReference table = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("table"), "");
        TableSourceReference tableB = new TableSourceReference(1, TableSourceReference.Type.TABLE, "", QualifiedName.of("tableB"), "b");
        Schema schema = Schema.of(ast("", Type.Any, table));
        Schema schemaB = Schema.of(ast("b", Type.Any, tableB));
        
        //@formatter:off
        ILogicalPlan expected = 
                projection(
                    new Join(
                        new Join(
                            ConstantScan.INSTANCE,
                            projection(
                                new MaxRowCountAssert(
                                    tableScan(schemaB, tableB),
                                    1),
                                asList(new AliasExpression(add(cre("col1", tableB), intLit(1)), "__expr0"))),
                            Join.Type.LEFT,
                            null,
                            (IExpression) null,
                            asSet(),
                            false,
                            Schema.EMPTY),
                        tableScan(schema, table),
                        Join.Type.LEFT,
                        null,
                        (IExpression) null,
                        asSet(),
                        true,
                        schema),
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
        
        TableSourceReference table = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("table"), "a");
        TableSourceReference tableB = new TableSourceReference(1, TableSourceReference.Type.TABLE, "", QualifiedName.of("tableB"), "b");
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
                        asSet(nast("col2", Type.Any, table)),
                        false,
                        schema),
                    asList(new AsteriskExpression(QualifiedName.EMPTY, null, Set.of(table)), new AliasExpression(cre("__expr0", tableB, ResolvedType.of(Type.Any)), "values")));
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

        ILogicalPlan plan = getColumnResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);
        
        TableSourceReference table = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("table"), "a");
        TableSourceReference tableB = new TableSourceReference(1, TableSourceReference.Type.TABLE, "", QualifiedName.of("tableB"), "b");
        TableSourceReference tableC = new TableSourceReference(2, TableSourceReference.Type.TABLE, "", QualifiedName.of("tableC"), "c");
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
                            false,
                            Schema.EMPTY),
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
                            asSet(nast("col2", Type.Any, table)),
                            false,
                            schema),
                        Join.Type.LEFT,
                        null,
                        (IExpression) null,
                        asSet(),
                        true,
                        schema),
                    asList(new AsteriskExpression(QualifiedName.EMPTY, null, Set.of(table)), new AliasExpression(
                            add(cre("__expr0", tableB, ResolvedType.of(Type.Any)), cre("__expr1", tableC, ResolvedType.of(Type.Any))), "value")));
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
