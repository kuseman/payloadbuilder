package se.kuseman.payloadbuilder.core.logicalplan.optimization;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static se.kuseman.payloadbuilder.core.utils.CollectionUtils.asSet;

import java.util.List;
import java.util.Random;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.core.expression.LiteralStringExpression;
import se.kuseman.payloadbuilder.core.logicalplan.Filter;
import se.kuseman.payloadbuilder.core.logicalplan.ILogicalPlan;
import se.kuseman.payloadbuilder.core.logicalplan.Join;
import se.kuseman.payloadbuilder.core.parser.Location;

/** Test of {@link ColumnOrdinalResolver} */
class ColumnOrdinalResolverTest extends ALogicalPlanOptimizerTest
{
    private ColumnResolver columnResolver = new ColumnResolver();
    private PredicatePushDown predicatePushDown = new PredicatePushDown();
    private ColumnOrdinalResolver rule = new ColumnOrdinalResolver();

    @Test
    void test_join_and_surrounding_filter_schema_full()
    {
        //@formatter:off
        String query =""
                + "select * "
                + "from stableA a "
                + "inner join stableB b "
                + "  on a.col3 = b.col3 "
                + "where a.col1 > b.col1 AND a.col2 = 'test' AND b.col2 > 100 ";
        //@formatter:on

        ILogicalPlan plan = optimizeWithPredicatePushDown(query);
        ILogicalPlan actual = optimize(plan);

        Schema resolvedSchemaStableA = new Schema(schemaSTableA.getColumns()
                .stream()
                .map(c -> col(c.getName(), c.getType(), sTableA))
                .collect(toList()));
        Schema resolvedSchemaStableB = new Schema(schemaSTableB.getColumns()
                .stream()
                .map(c -> col(c.getName(), c.getType(), sTableB))
                .collect(toList()));

        //@formatter:off
        ILogicalPlan expected =
                projection(
                    new Filter(
                        new Join(
                            new Filter(
                                tableScan(resolvedSchemaStableA, sTableA),
                                sTableA,
                                eq(cre("col2", sTableA, 1, ResolvedType.of(Type.String)), new LiteralStringExpression("test"))),
                            new Filter(
                                tableScan(resolvedSchemaStableB, sTableB),
                                sTableB,
                                gt(cre("col2", sTableB, 1 /*4*/, ResolvedType.of(Type.String)), intLit(100))),        // This has ordinal 4 before running rule
                            Join.Type.INNER,
                            null,
                            eq(cre("col3", sTableA, 2, ResolvedType.of(Type.Float)), cre("col3", sTableB, 5, ResolvedType.of(Type.Float))),
                            asSet(),
                            false,
                            Schema.EMPTY),
                        null,
                        gt(cre("col1", sTableA, 0, ResolvedType.of(Type.Int)), cre("col1", sTableB, 3, ResolvedType.of(Type.Boolean)))),
                List.of(cre("col1", sTableA, 0, ResolvedType.INT),
                        cre("col2", sTableA, 1, ResolvedType.STRING),
                        cre("col3", sTableA, 2, ResolvedType.FLOAT),
                        cre("col1", sTableB, 3, ResolvedType.BOOLEAN),
                        cre("col2", sTableB, 4, ResolvedType.STRING),
                        cre("col3", sTableB, 5, ResolvedType.FLOAT)
                        ));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    private ILogicalPlan optimizeWithPredicatePushDown(String query)
    {
        ILogicalPlan plan = getSchemaResolvedPlan(query);

        ALogicalPlanOptimizer.Context crCtx = columnResolver.createContext(context);
        plan = columnResolver.optimize(crCtx, plan);

        PredicatePushDown.Ctx ppCtx = predicatePushDown.createContext(context);
        plan = predicatePushDown.optimize(ppCtx, plan);
        return plan;
    }

    private ILogicalPlan optimize(ILogicalPlan plan)
    {
        ColumnOrdinalResolver.Ctx ctx = rule.createContext(context);
        return rule.optimize(ctx, plan);
    }
}
