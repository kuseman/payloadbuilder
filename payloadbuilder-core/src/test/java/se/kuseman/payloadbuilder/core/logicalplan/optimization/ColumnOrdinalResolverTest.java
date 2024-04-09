package se.kuseman.payloadbuilder.core.logicalplan.optimization;

import static java.util.stream.Collectors.toList;
import static se.kuseman.payloadbuilder.core.utils.CollectionUtils.asSet;

import java.util.List;
import java.util.Random;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn;
import se.kuseman.payloadbuilder.core.expression.AsteriskExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralStringExpression;
import se.kuseman.payloadbuilder.core.logicalplan.Filter;
import se.kuseman.payloadbuilder.core.logicalplan.ILogicalPlan;
import se.kuseman.payloadbuilder.core.logicalplan.Join;
import se.kuseman.payloadbuilder.core.logicalplan.Projection;
import se.kuseman.payloadbuilder.core.parser.Location;

/** Test of {@link ColumnOrdinalResolver} */
public class ColumnOrdinalResolverTest extends ALogicalPlanOptimizerTest
{
    private ColumnResolver columnResolver = new ColumnResolver();
    private PredicatePushDown predicatePushDown = new PredicatePushDown();
    private ColumnOrdinalResolver rule = new ColumnOrdinalResolver();

    @Test
    public void test_join_and_surrounding_filter_schema_full()
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
                .map(c -> new CoreColumn(c, sTableA))
                .collect(toList()));
        Schema resolvedSchemaStableB = new Schema(schemaSTableB.getColumns()
                .stream()
                .map(c -> new CoreColumn(c, sTableB))
                .collect(toList()));

        //@formatter:off
        ILogicalPlan expected =
                new Projection(
                    new Filter(
                        new Join(
                            new Filter(
                                tableScan(resolvedSchemaStableA, sTableA),
                                sTableA,
                                eq(cre(stACol2, 1, ResolvedType.of(Type.String)), new LiteralStringExpression("test"))),
                            new Filter(
                                tableScan(resolvedSchemaStableB, sTableB),
                                sTableB,
                                gt(cre(stBCol2, 1 /*4*/, ResolvedType.of(Type.String)), intLit(100))),        // This has ordinal 4 before running rule
                            Join.Type.INNER,
                            null,
                            eq(cre(stACol3, 2, ResolvedType.of(Type.Float)), cre(stBCol3, 5, ResolvedType.of(Type.Float))),
                            asSet(),
                            false),
                        null,
                        gt(cre(stACol1, 0, ResolvedType.of(Type.Int)), cre(stBCol1, 3, ResolvedType.of(Type.Boolean)))
                    ),
                    List.of(new AsteriskExpression(QualifiedName.of(), null, Set.of(sTableA, sTableB))),
                    false);
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
