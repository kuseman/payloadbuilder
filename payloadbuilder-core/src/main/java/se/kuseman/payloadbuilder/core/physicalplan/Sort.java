package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.entry;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.ofEntries;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.NullOrder;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.Order;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.core.QueryException;
import se.kuseman.payloadbuilder.core.common.DescribableNode;
import se.kuseman.payloadbuilder.core.common.SortItem;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.execution.ValueVectorAdapter;
import se.kuseman.payloadbuilder.core.execution.VectorUtils;
import se.kuseman.payloadbuilder.core.expression.LiteralIntegerExpression;

import it.unimi.dsi.fastutil.ints.IntArrays;

/** Sort plan */
public class Sort implements IPhysicalPlan
{
    private final int nodeId;
    private final IPhysicalPlan input;
    private final List<SortItem> sortItems;

    public Sort(int nodeId, IPhysicalPlan input, List<SortItem> sortItems)
    {
        this.nodeId = nodeId;
        this.input = requireNonNull(input, "input");
        this.sortItems = requireNonNull(sortItems, "sortItems");
    }

    @Override
    public int getNodeId()
    {
        return nodeId;
    }

    @Override
    public String getName()
    {
        return "Sort";
    }

    @Override
    public Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        return ofEntries(true, entry("Order By", sortItems.stream()
                .map(si -> si.getExpression()
                        .toString() + " " + si.getOrder())
                .collect(toList())), entry(IDatasource.OUTPUT, DescribeUtils.getOutputColumns(input.getSchema())));
    }

    @Override
    public Schema getSchema()
    {
        return input.getSchema();
    }

    @Override
    public TupleIterator execute(IExecutionContext context)
    {
        TupleIterator it = input.execute(context);
        TupleVector all = PlanUtils.concat(((ExecutionContext) context).getBufferAllocator(), it);

        if (all.getRowCount() == 0)
        {
            return TupleIterator.EMPTY;
        }

        final int itemSize = sortItems.size();
        final ValueVector[] expressionVectors = new ValueVector[itemSize];

        final int rowCount = all.getRowCount();
        // Result array to store the resulting sorted index of the value vector
        // Start with input order
        final int[] sortIndices = IntStream.range(0, rowCount)
                .toArray();

        IntArrays.mergeSort(sortIndices, (a, b) ->
        {
            for (int i = 0; i < itemSize; i++)
            {
                SortItem sortItem = sortItems.get(i);
                ValueVector vals = expressionVectors[i];
                // Eval expression if not already done
                if (vals == null)
                {
                    if (sortItem.getExpression() instanceof LiteralIntegerExpression)
                    {
                        int index = ((LiteralIntegerExpression) sortItem.getExpression()).getValue();
                        if (index <= 0
                                || index > all.getSchema()
                                        .getSize())
                        {
                            throw new QueryException("ORDER BY position is out of range");
                        }

                        // Ordinal sort is 1 based
                        vals = all.getColumn(index - 1);
                    }
                    else
                    {
                        vals = sortItem.getExpression()
                                .eval(all, context);
                    }
                    expressionVectors[i] = vals;
                }

                ResolvedType type = vals.type();

                // CSOFF
                boolean aIsNull = vals.isNull(a);
                boolean bIsNull = vals.isNull(b);
                // CSON
                NullOrder nullOrder = sortItem.getNullOrder();

                if (aIsNull
                        && bIsNull)
                {
                    continue;
                }
                else if (aIsNull)
                {
                    // Null is always less if not specified
                    return nullOrder == NullOrder.FIRST
                            || nullOrder == NullOrder.UNDEFINED ? -1
                                    : 1;
                }
                else if (bIsNull)
                {
                    // Null is always less if not specified
                    return nullOrder == NullOrder.FIRST
                            || nullOrder == NullOrder.UNDEFINED ? 1
                                    : -1;
                }

                Order order = sortItem.getOrder();
                int c = VectorUtils.compare(vals, vals, type.getType(), a, b);
                if (c != 0)
                {
                    return c * (order == Order.DESC ? -1
                            : 1);
                }
            }
            return 0;
        });

        int columnCount = all.getSchema()
                .getSize();
        final ValueVector[] columns = new ValueVector[columnCount];
        for (int i = 0; i < columnCount; i++)
        {
            columns[i] = new ValueVectorAdapter(all.getColumn(i))
            {
                @Override
                protected int getRow(int row)
                {
                    return sortIndices[row];
                }
            };
        }

        return TupleIterator.singleton(new TupleVector()
        {
            @Override
            public Schema getSchema()
            {
                return all.getSchema();
            }

            @Override
            public int getRowCount()
            {
                return rowCount;
            }

            @Override
            public ValueVector getColumn(int column)
            {
                return columns[column];
            }
        });
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
        else if (obj instanceof Sort)
        {
            Sort that = (Sort) obj;
            return nodeId == that.nodeId
                    && input.equals(that.input)
                    && sortItems.equals(that.sortItems);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "Sort (" + nodeId
               + "): "
               + sortItems.stream()
                       .map(i -> i.getExpression()
                               .toVerboseString() + " " + i.getOrder())
                       .collect(joining(", "));
    }

}
