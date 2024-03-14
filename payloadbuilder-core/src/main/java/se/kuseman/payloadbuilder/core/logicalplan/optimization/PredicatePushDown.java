package se.kuseman.payloadbuilder.core.logicalplan.optimization;

import static java.util.Collections.emptySet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.expression.PredicateAnalyzer;
import se.kuseman.payloadbuilder.core.expression.PredicateAnalyzer.AnalyzePair;
import se.kuseman.payloadbuilder.core.expression.PredicateAnalyzer.AnalyzeResult;
import se.kuseman.payloadbuilder.core.logicalplan.ALogicalPlanVisitor;
import se.kuseman.payloadbuilder.core.logicalplan.Filter;
import se.kuseman.payloadbuilder.core.logicalplan.ILogicalPlan;
import se.kuseman.payloadbuilder.core.logicalplan.Join;
import se.kuseman.payloadbuilder.core.logicalplan.Join.Type;
import se.kuseman.payloadbuilder.core.logicalplan.TableFunctionScan;
import se.kuseman.payloadbuilder.core.logicalplan.TableScan;

/** Plan rewriter that pushes predicates down to scan operators */
class PredicatePushDown extends ALogicalPlanOptimizer<PredicatePushDown.Ctx>
{
    PredicatePushDown()
    {
        // No expression rewriting here
        super(null);
    }
    /*
     * @formatter:off
     * 
     * select *
     * from tableA a
     * inner join tableB b
     *   on b.col1 = a.col1
     * where a.col > 1
     * and b.col < 10
     * 
     * filter (a.col > 1 and b.col < 10)
     *   join: (b.col1 = a.col1)
     *     scan: tableA
     *     scan: tableB
     *  
     *  ==>
     *  
     *  join: (b.col1 = a.col1)
     *    filter: (a.col > 1)
     *      scan: tableA
     *    filter: (b.col < 10)
     *      scan: tableB
     * 
     *  
     * @formatter:on
     */

    static class Ctx extends ALogicalPlanOptimizer.Context
    {
        /**
         * <pre>
         * Set with constrained table sources that is used to limit which table sources
         * that can extract predicates. For example we don't want to push a WHERE into a LEFT JOIN
         * Ie.
         *
         *  select *
         *  from tableA A
         *  left join tableB b
         *   on a.col = b.col
         *   and a.value = 1         <-- Cannot push down to tableA a
         *   and b.value = 1         <-- Can push down to tableB b
         *  where b.field = 1        <-- Cannot push down to tableB b
         *
         *  1. Where
         *     Extract b.field = 1 with constraint: None
         *  2. Join (Left => constrain predicates to all inner table sources)
         *     Extract a.vlaue = 1 with constraint: tableB
         *     Extract b.value = 1 with constraint: tableB
         *     Set constrainedTableSources to tableB
         *  4. Create tableA
         *     Fetch predicates for tableA => none
         *  4. Create tableB
         *     Fetch predicates for tableB => Only match is b.value = 1
         * </pre>
         */
        Set<TableSourceReference> constrainedTableSources = new HashSet<>();

        /**
         * List with pair of analyzed predicates (from WHERE or JOIN etc.) along with a set of allowed push down table sources
         * 
         * <pre>
         * The allowed table sources is used when having a left join
         * to disallow push downs from the joined condition to the outer table source.
         * ie.
         *
         * select *
         * from tableA a
         * left join tableB b
         *   on b.col = a.col
         *   and a.value > 10 <--- this one cannot be pushed down to tableA since it's a left join and hence tableA is disallowed
         *                         only pushdowns to the inner table source (tableB) are allowed
         * </pre>
         */
        List<Pair<Set<TableSourceReference>, AnalyzeResult>> analyzeResults = null;

        Ctx(IExecutionContext context)
        {
            super(context);
        }

        /** Add analyze result returning the index of the result for later use */
        int add(AnalyzeResult analyzeResult, Set<TableSourceReference> constrainedTableSources)
        {
            if (analyzeResults == null)
            {
                analyzeResults = new ArrayList<>();
            }
            analyzeResults.add(Pair.of(constrainedTableSources, analyzeResult));
            return analyzeResults.size() - 1;
        }

        /** Return predicate for analyze result at provided index */
        AnalyzeResult getAnalyzeResult(int index)
        {
            return analyzeResults.get(index)
                    .getValue();
        }

        /** Extract all analyze pairs for provided alias */
        List<AnalyzePair> extractPushDownPredicate(TableSourceReference tableSource)
        {
            if (analyzeResults == null)
            {
                return null;
            }

            // If we have a constraint on input table source
            // we can only extract those anaylyze results that has that table source constrained
            // else we can only extract those results with no constraints
            boolean checkConstrainedTableSources = constrainedTableSources.contains(tableSource);

            // Extract pairs from all analyze result and combine a predicate
            List<AnalyzePair> pairs = new ArrayList<>();
            int size = analyzeResults.size();
            for (int i = 0; i < size; i++)
            {
                Pair<Set<TableSourceReference>, AnalyzeResult> p = analyzeResults.get(i);

                // Analyze result is constrained to table sources that is not among input table source => move on
                if (checkConstrainedTableSources
                        && !p.getKey()
                                .contains(tableSource))
                {
                    continue;
                }
                // or we don't have any constraints on input table source but the analyze result has => move on
                else if (!checkConstrainedTableSources
                        && !p.getKey()
                                .isEmpty())
                {
                    continue;
                }

                Pair<List<AnalyzePair>, AnalyzeResult> pair = p.getValue()
                        .extractPushdownPairs(tableSource);
                if (pair != null)
                {
                    // Collect the pairs
                    pairs.addAll(pair.getKey());
                    // Put back the left overs for plans to use upon visit return
                    analyzeResults.set(i, Pair.of(p.getKey(), pair.getValue()));
                }
            }
            if (pairs.isEmpty())
            {
                return null;
            }
            return pairs;
        }
    }

    @Override
    Ctx createContext(IExecutionContext context)
    {
        return new Ctx(context);
    }

    @Override
    ILogicalPlan optimize(Context context, ILogicalPlan plan)
    {
        return plan.accept(this, (Ctx) context);
    }

    @Override
    public ILogicalPlan visit(Filter plan, Ctx context)
    {
        // Analyze predicate
        int resultIndex = context.add(PredicateAnalyzer.analyze(plan.getPredicate()), emptySet());

        // Visit the input
        ILogicalPlan input = plan.getInput()
                .accept(this, context);

        // Extract the predicate that is left in context
        AnalyzeResult analyzeResult = context.getAnalyzeResult(resultIndex);
        IExpression predicate = analyzeResult.getPredicate();

        // If all predicate pairs was used then no filter should be used here, return input
        if (predicate == null)
        {
            return input;
        }

        return new Filter(input, plan.getTableSource(), predicate);
    }

    @Override
    public ILogicalPlan visit(TableScan plan, Ctx context)
    {
        TableSourceReference tableSource = plan.getTableSource();
        // Push down the predicate to here, creating a filter above the table
        List<AnalyzePair> pairs = context.extractPushDownPredicate(tableSource);
        if (pairs != null)
        {
            IExpression predicate = AnalyzeResult.getPredicate(pairs);
            return new Filter(plan, plan.getTableSource(), predicate);
        }

        // No predicates was found, just return the input plan since it won't be altered
        return plan;
    }

    @Override
    public ILogicalPlan visit(TableFunctionScan plan, Ctx context)
    {
        TableSourceReference tableSource = plan.getTableSource();
        // Push down the predicate to here, creating a filter above the table function
        List<AnalyzePair> pairs = context.extractPushDownPredicate(tableSource);
        if (pairs != null)
        {
            IExpression predicate = AnalyzeResult.getPredicate(pairs);
            return new Filter(plan, plan.getTableSource(), predicate);
        }

        // No predicates was found, just return the input plan since it won't be altered
        return plan;
    }

    @Override
    public ILogicalPlan visit(Join plan, Ctx context)
    {
        boolean isLeft = plan.getType() == Type.LEFT;

        Set<TableSourceReference> constrainedTableSoueces = emptySet();

        // We don't push anything down to a LEFT join
        // That is up to the user to optimize at the moment.
        if (isLeft)
        {
            constrainedTableSoueces = new HashSet<>();
            plan.getInner()
                    .accept(TableSourceVisitor.INSTANCE, constrainedTableSoueces);
        }

        Set<TableSourceReference> prevConstrainedTableSources = new HashSet<>(context.constrainedTableSources);

        context.constrainedTableSources.addAll(constrainedTableSoueces);

        // Add to result then visit down stream plans
        // CSOFF
        int resultIndex = context.add(PredicateAnalyzer.analyze(plan.getCondition()), constrainedTableSoueces);
        // CSON

        ILogicalPlan outer = plan.getOuter()
                .accept(this, context);
        ILogicalPlan inner = plan.getInner()
                .accept(this, context);

        // Restore previous values
        context.constrainedTableSources = prevConstrainedTableSources;

        // Extract remaining predicate pairs and use when re-creating join
        AnalyzeResult analyzeResult = context.analyzeResults.remove(resultIndex)
                .getValue();

        return new Join(outer, inner, plan.getType(), plan.getPopulateAlias(), analyzeResult.getPredicate(), plan.getOuterReferences(), plan.isSwitchedInputs(), plan.getOuterSchema());
    }

    /** Visitor that collects table sources from a {@link ILogicalPlan} */
    static class TableSourceVisitor extends ALogicalPlanVisitor<Void, Set<TableSourceReference>>
    {
        private static final TableSourceVisitor INSTANCE = new TableSourceVisitor();

        @Override
        public Void visit(TableScan plan, Set<TableSourceReference> context)
        {
            context.add(plan.getTableSource());
            return super.visit(plan, context);
        }

        @Override
        public Void visit(TableFunctionScan plan, Set<TableSourceReference> context)
        {
            context.add(plan.getTableSource());
            return super.visit(plan, context);
        }
    }
}
