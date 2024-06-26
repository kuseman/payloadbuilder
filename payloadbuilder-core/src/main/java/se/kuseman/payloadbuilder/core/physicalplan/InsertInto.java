package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.function.Supplier;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Index;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.QueryException;
import se.kuseman.payloadbuilder.core.common.DescribableNode;
import se.kuseman.payloadbuilder.core.execution.QuerySession;
import se.kuseman.payloadbuilder.core.execution.StatementContext;
import se.kuseman.payloadbuilder.core.execution.TemporaryTable;

/** Plan that inserts input's output into a temporary table */
public class InsertInto implements IPhysicalPlan
{
    private static final QualifiedName CACHEPREFIX = QualifiedName.of("cacheprefix");
    private static final QualifiedName CACHETTL = QualifiedName.of("cachettl");

    private final int nodeId;
    private final IPhysicalPlan input;
    private final QualifiedName tableName;
    private final List<Index> indices;
    private final IExpression cachePrefixExpression;
    private final IExpression cacheTtlExpression;

    public InsertInto(int nodeId, IPhysicalPlan input, QualifiedName tableName, List<Index> indices, List<Option> options)
    {
        this.nodeId = nodeId;
        this.input = requireNonNull(input, "input");
        this.tableName = requireNonNull(tableName, "tableName");
        this.indices = requireNonNull(indices, "indices");

        IExpression cachePrefixExpression = null;
        IExpression cacheTtlExpression = null;

        int size = options.size();
        for (int i = 0; i < size; i++)
        {
            Option option = options.get(i);
            if (option.getOption()
                    .equalsIgnoreCase(CACHEPREFIX))
            {
                cachePrefixExpression = option.getValueExpression();
            }
            else if (option.getOption()
                    .equalsIgnoreCase(CACHETTL))
            {
                cacheTtlExpression = option.getValueExpression();
            }
        }

        this.cachePrefixExpression = cachePrefixExpression;
        this.cacheTtlExpression = cacheTtlExpression;

        if (cachePrefixExpression != null
                && cacheTtlExpression == null)
        {
            throw new IllegalArgumentException("CacheTTL hint is mandatory to enable cache on temporary table: #" + tableName);
        }
    }

    @Override
    public boolean hasWritableOutput()
    {
        return false;
    }

    @Override
    public int getNodeId()
    {
        return nodeId;
    }

    @Override
    public String getName()
    {
        return "Insert Into";
    }

    @Override
    public Schema getSchema()
    {
        return input.getSchema();
    }

    @Override
    public TupleIterator execute(IExecutionContext context)
    {
        // Find out if result should be cached
        QualifiedName cacheName = null;
        Duration cacheTtl = null;

        if (cacheTtlExpression != null)
        {
            ValueVector vector;
            if (cachePrefixExpression != null)
            {
                vector = cachePrefixExpression.eval(TupleVector.CONSTANT, context);
                if (vector.isNull(0))
                {
                    throw new QueryException("Cache prefix expression: " + cachePrefixExpression + " evaluated to null");
                }
                cacheName = tableName.prepend(vector.valueAsString(0));
            }
            else
            {
                cacheName = tableName;
            }

            vector = cacheTtlExpression.eval(TupleVector.CONSTANT, context);

            try
            {
                cacheTtl = vector.isNull(0) ? null
                        : Duration.parse(String.valueOf(vector.valueAsObject(0)));
            }
            catch (DateTimeParseException e)
            {
                throw new IllegalArgumentException(String.valueOf(vector.valueAsObject(0)) + " cannot be parsed as a Duration. See java.time.Duration#parse");
            }
        }

        Supplier<TemporaryTable> tempTableSupplier = () -> new TemporaryTable(PlanUtils.concat(context, input.execute(context)), indices);
        TemporaryTable temporaryTable;
        if (cacheTtl != null)
        {
            temporaryTable = ((QuerySession) context.getSession()).getTempTableCache()
                    .computIfAbsent(cacheName, cacheTtl, tempTableSupplier);
        }
        else
        {
            temporaryTable = tempTableSupplier.get();
        }

        ((QuerySession) context.getSession()).setTemporaryTable(tableName, temporaryTable);
        ((StatementContext) context.getStatementContext()).setRowCount(temporaryTable.getTupleVector()
                .getRowCount());

        return TupleIterator.EMPTY;
    }

    @Override
    public List<IPhysicalPlan> getChildren()
    {
        return singletonList(input);
    }

    @Override
    public List<DescribableNode> getChildNodes()
    {
        return singletonList(input);
    }
}
