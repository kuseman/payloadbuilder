package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.IDatasink;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.core.QueryException;
import se.kuseman.payloadbuilder.core.common.DescribableNode;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;
import se.kuseman.payloadbuilder.core.execution.StatementContext;

/** Physical plan of an insert into statement. */
public class InsertInto implements IPhysicalPlan
{
    private final int nodeId;
    private final List<String> insertColumns;
    private final IPhysicalPlan input;
    private final boolean hasAsteriskSchemaOrInput;
    private final IDatasink datasink;

    public InsertInto(int nodeId, IPhysicalPlan input, List<String> insertColumns, IDatasink datasink)
    {
        this.nodeId = nodeId;
        this.input = requireNonNull(input, "input");
        this.insertColumns = insertColumns;
        this.datasink = requireNonNull(datasink, " datasink");
        this.hasAsteriskSchemaOrInput = SchemaUtils.isAsterisk(input.getSchema(), true);
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
    public boolean hasWritableOutput()
    {
        return false;
    }

    @Override
    public TupleIterator execute(IExecutionContext context)
    {
        LazyTupleIterator iterator = new LazyTupleIterator(context);
        try
        {
            datasink.execute(context, iterator);
        }
        finally
        {
            // Make sure we always close the iterator to guard against bad implementations
            iterator.close();
        }
        ((StatementContext) context.getStatementContext()).setRowCount(iterator.rowCount);
        return TupleIterator.EMPTY;
    }

    @Override
    public List<IPhysicalPlan> getChildren()
    {
        return List.of(input);
    }

    @Override
    public List<? extends DescribableNode> getChildNodes()
    {
        return List.of(input);
    }

    @Override
    public int hashCode()
    {
        return nodeId;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        else if (obj == this)
        {
            return true;
        }
        else if (obj instanceof InsertInto that)
        {
            return nodeId == that.nodeId
                    && insertColumns.equals(that.insertColumns)
                    && input.equals(that.input)
                    && datasink.equals(that.datasink);
        }
        return false;
    }

    /**
     * Lazy iterator that executes the input on first access. This to allow for {@link IDatasink}'s to handle caches etc. without the need for executing anything if not needed.
     */
    private class LazyTupleIterator implements TupleIterator
    {
        private final IExecutionContext context;
        private TupleIterator iterator;
        private int rowCount;
        private boolean closed;

        LazyTupleIterator(IExecutionContext context)
        {
            this.context = context;
        }

        @Override
        public int estimatedBatchCount()
        {
            return getIterator().estimatedBatchCount();
        }

        @Override
        public int estimatedRowCount()
        {
            return getIterator().estimatedRowCount();
        }

        @Override
        public void close()
        {
            if (!closed
                    && iterator != null)
            {
                iterator.close();
                closed = true;
            }
        }

        @Override
        public boolean hasNext()
        {
            return getIterator().hasNext();
        }

        @Override
        public TupleVector next()
        {
            TupleVector next = getIterator().next();

            if (insertColumns != null
                    && next.getSchema()
                            .getSize() != insertColumns.size())
            {
                throw new QueryException("Insert column count doesn't match input column count. Insert columns: " + insertColumns
                                         + ", input columns: "
                                         + next.getSchema()
                                                 .getColumns());
            }
            rowCount += next.getRowCount();

            // Create a proxy vector if the input is asterisk to have a proper schema with meta etc.
            // that is only known runtime
            if (hasAsteriskSchemaOrInput)
            {
                next = createProxyVector(next);
            }

            return next;
        }

        private TupleIterator getIterator()
        {
            if (iterator == null)
            {
                iterator = input.execute(context);
            }
            return iterator;
        }

        private TupleVector createProxyVector(TupleVector tv)
        {
            return new TupleVector()
            {
                @Override
                public Schema getSchema()
                {
                    return SchemaUtils.rewriteSchema(tv.getSchema(), (StatementContext) context.getStatementContext());
                }

                @Override
                public int getRowCount()
                {
                    return tv.getRowCount();
                }

                @Override
                public ValueVector getColumn(int column)
                {
                    return tv.getColumn(column);
                }
            };
        }
    }
}
