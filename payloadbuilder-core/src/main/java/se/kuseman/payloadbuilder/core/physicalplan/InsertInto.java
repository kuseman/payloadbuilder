package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.lowerCase;

import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.function.Supplier;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Index;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.common.DescribableNode;
import se.kuseman.payloadbuilder.core.common.Option;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.execution.QuerySession;
import se.kuseman.payloadbuilder.core.execution.StatementContext;
import se.kuseman.payloadbuilder.core.execution.TemporaryTable;

/** Plan that inserts input's output into a temporary table */
public class InsertInto implements IPhysicalPlan
{
    private final int nodeId;
    private final IPhysicalPlan input;
    private final String tableName;
    private final List<Index> indices;
    private final IExpression cacheNameExpression;
    private final IExpression cacheKeyExpression;
    private final IExpression cacheTtlExpression;

    public InsertInto(int nodeId, IPhysicalPlan input, String tableName, List<Index> indices, List<Option> options)
    {
        this.nodeId = nodeId;
        this.input = requireNonNull(input, "input");
        this.tableName = requireNonNull(tableName, "tableName");
        this.indices = requireNonNull(indices, "indices");

        IExpression cacheNameExpression = null;
        IExpression cacheKeyExpression = null;
        IExpression cacheTtlExpression = null;

        int size = options.size();
        for (int i = 0; i < size; i++)
        {
            Option option = options.get(i);
            String lowerName = lowerCase(option.getOption()
                    .toDotDelimited());
            if (lowerName.equals("cachename"))
            {
                cacheNameExpression = option.getValueExpression();
            }
            else if (lowerName.equals("cachekey"))
            {
                cacheKeyExpression = option.getValueExpression();
            }
            else if (lowerName.equals("cachettl"))
            {
                cacheTtlExpression = option.getValueExpression();
            }
        }

        this.cacheNameExpression = cacheNameExpression;
        this.cacheKeyExpression = cacheKeyExpression;
        this.cacheTtlExpression = cacheTtlExpression;
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
        Object cacheKey = null;
        Duration cacheTtl = null;

        if (cacheNameExpression != null)
        {
            ValueVector vector = cacheNameExpression.eval(TupleVector.CONSTANT, context);
            cacheName = vector.isNull(0) ? null
                    : QualifiedName.of(vector.valueAsObject(0));

            if (cacheKeyExpression != null)
            {
                vector = cacheKeyExpression.eval(TupleVector.CONSTANT, context);
                cacheKey = vector.isNull(0) ? null
                        : vector.valueAsObject(0);
            }

            if (cacheTtlExpression != null)
            {
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
        }

        Supplier<TemporaryTable> tempTableSupplier = () -> new TemporaryTable(PlanUtils.concat(((ExecutionContext) context).getBufferAllocator(), input.execute(context)), indices);
        TemporaryTable temporaryTable;
        if (cacheName != null
                && cacheKey != null)
        {
            temporaryTable = ((QuerySession) context.getSession()).getTempTableCache()
                    .computIfAbsent(cacheName, cacheKey, cacheTtl, tempTableSupplier);
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
