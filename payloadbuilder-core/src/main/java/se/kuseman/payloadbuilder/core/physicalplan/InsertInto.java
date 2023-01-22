package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.lowerCase;

import java.time.Duration;
import java.util.List;
import java.util.function.Supplier;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TupleIterator;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.QuerySession;
import se.kuseman.payloadbuilder.core.common.DescribableNode;
import se.kuseman.payloadbuilder.core.common.Option;
import se.kuseman.payloadbuilder.core.execution.StatementContext;

/** Plan that inserts input's output into a temporary table */
public class InsertInto implements IPhysicalPlan
{
    private final int nodeId;
    private final IPhysicalPlan input;
    private final String tableName;
    private final IExpression cacheNameExpression;
    private final IExpression cacheKeyExpression;
    private final IExpression cacheTtlExpression;

    public InsertInto(int nodeId, IPhysicalPlan input, String tableName, List<Option> options)
    {
        this.nodeId = nodeId;
        this.input = requireNonNull(input, "input");
        this.tableName = requireNonNull(tableName, "tableName");

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
            cacheName = QualifiedName.of(cacheNameExpression.eval(TupleVector.CONSTANT, context)
                    .valueAsObject(0));
            cacheKey = cacheKeyExpression != null ? cacheKeyExpression.eval(TupleVector.CONSTANT, context)
                    .valueAsObject(0)
                    : null;
            cacheTtl = cacheTtlExpression != null ? Duration.parse((String) cacheTtlExpression.eval(TupleVector.CONSTANT, context)
                    .valueAsObject(0))
                    : null;
        }

        Supplier<TupleVector> tempTableSupplier = () -> PlanUtils.concat(input.execute(context));
        TupleVector tupleVector;
        if (cacheName != null)
        {
            tupleVector = ((QuerySession) context.getSession()).getTempTableCache()
                    .computIfAbsent(cacheName, cacheKey, cacheTtl, tempTableSupplier);
        }
        else
        {
            tupleVector = tempTableSupplier.get();
        }

        ((QuerySession) context.getSession()).setTemporaryTable(tableName, tupleVector);
        ((StatementContext) context.getStatementContext()).setRowCount(tupleVector.getRowCount());

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
