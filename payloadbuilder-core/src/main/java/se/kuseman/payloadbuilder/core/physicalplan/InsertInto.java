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
        this.hasAsteriskSchemaOrInput = SchemaUtils.originatesFromAsteriskInput(input.getSchema());
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
        int[] rowCount = new int[1];
        boolean[] closed = new boolean[1];
        TupleIterator[] underlying = new TupleIterator[1];

        try
        {
            datasink.execute(context, () ->
            {
                TupleIterator it = input.execute(context);
                underlying[0] = it;
                return new TupleIterator()
                {
                    @Override
                    public int estimatedBatchCount()
                    {
                        return it.estimatedBatchCount();
                    }

                    @Override
                    public int estimatedRowCount()
                    {
                        return it.estimatedRowCount();
                    }

                    @Override
                    public boolean hasNext()
                    {
                        return it.hasNext();
                    }

                    @Override
                    public TupleVector next()
                    {
                        TupleVector next = it.next();
                        if (insertColumns != null
                                && next.getSchema()
                                        .getSize() != insertColumns.size())
                        {
                            throw new QueryException("Insert column count doesn't match input column count. Insert columns: " + insertColumns
                                                     + ", input columns: "
                                                     + next.getSchema()
                                                             .getColumns());
                        }
                        rowCount[0] += next.getRowCount();
                        return hasAsteriskSchemaOrInput ? createProxyVector(context, next)
                                : next;
                    }

                    @Override
                    public void close()
                    {
                        if (!closed[0])
                        {
                            closed[0] = true;
                            it.close();
                        }
                    }
                };
            });
        }
        finally
        {
            // Guard against datasink implementations that forget to close the iterator
            if (underlying[0] != null
                    && !closed[0])
            {
                closed[0] = true;
                underlying[0].close();
            }
        }

        ((StatementContext) context.getStatementContext()).setRowCount(rowCount[0]);
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

    private TupleVector createProxyVector(IExecutionContext context, TupleVector tv)
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
