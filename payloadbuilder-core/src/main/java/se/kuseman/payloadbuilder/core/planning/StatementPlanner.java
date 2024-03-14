package se.kuseman.payloadbuilder.core.planning;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.ISortItem;
import se.kuseman.payloadbuilder.api.catalog.TableSchema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.execution.QuerySession;
import se.kuseman.payloadbuilder.core.expression.PredicateAnalyzer.AnalyzePair;
import se.kuseman.payloadbuilder.core.logicalplan.ILogicalPlan;
import se.kuseman.payloadbuilder.core.statement.QueryStatement;

/** Statement planner that transforms a {@link QueryStatement} to an executable form */
public class StatementPlanner
{
    private static final StatementRewriter STATEMENT_REWRITER = new StatementRewriter();

    static class Context
    {
        final ExecutionContext context;
        int nodeIdCounter;
        Map<TableSourceReference, TableSourcePushDown> tableSourcePushDown = new HashMap<>();

        /** Flag indicating that all joins must preserve it's outer order because a sort was removed and consumed by a catalog */
        boolean joinPreserveOuterOrder;
        boolean topTableScanVisited;

        Map<QualifiedName, TableSchema> schemaByTempTable = new HashMap<>();

        Map<TableSourceReference, TableSchema> schemaByTableSource = new HashMap<>();

        /** Flag indicating that next plan to be generated is an analzye */
        boolean analyze;

        /** Field with the last created logical plan. Used when holding schema information for temp tables etc. */
        ILogicalPlan currentLogicalPlan;

        /** Seek predicate that should be used when creating a table operator. */
        SeekPredicate seekPredicate;

        Context(IExecutionContext context)
        {
            this.context = requireNonNull((ExecutionContext) context);
        }

        int getNextNodeId()
        {
            int result = nodeIdCounter;
            nodeIdCounter++;
            return result;
        }

        public QuerySession getSession()
        {
            return context.getSession();
        }
    }

    static class TableSourcePushDown
    {
        /** If we have sort on a single table source it can be pushed to catalog */
        List<? extends ISortItem> sortItems = emptyList();

        /** Push down predicates that can be consumed by catalog */
        List<AnalyzePair> predicatePairs = emptyList();
    }

    /** Plans provided query. Produces a runnable query that can be cached */
    public static QueryStatement plan(QuerySession session, QueryStatement query)
    {
        ExecutionContext context = new ExecutionContext(session);
        Context ctx = new Context(context);

        // Add existing session tables into context
        // This is used when working in a state full session system like Queryeer
        // where the session is reused across executions.
        session.getTemporaryTables()
                .forEach(e -> ctx.schemaByTempTable.put(e.getKey(), new TableSchema(e.getValue()
                        .getTupleVector()
                        .getSchema(),
                        e.getValue()
                                .getIndices())));

        return new QueryStatement(query.getStatements()
                .stream()
                .map(s -> s.accept(STATEMENT_REWRITER, ctx))
                .collect(toList()));
    }
}
