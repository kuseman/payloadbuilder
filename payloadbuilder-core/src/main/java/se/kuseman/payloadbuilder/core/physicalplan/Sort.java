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
import se.kuseman.payloadbuilder.api.catalog.TupleIterator;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.UTF8String;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVectorAdapter;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.utils.ExpressionMath;
import se.kuseman.payloadbuilder.core.QueryException;
import se.kuseman.payloadbuilder.core.common.DescribableNode;
import se.kuseman.payloadbuilder.core.common.SortItem;
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
        TupleVector all = PlanUtils.concat(it);

        if (all.getRowCount() == 0)
        {
            return TupleIterator.EMPTY;
        }

        int itemSize = sortItems.size();
        ValueVector[] expressionVectors = new ValueVector[itemSize];

        int rowCount = all.getRowCount();
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
                boolean aIsNull = false;
                boolean bIsNull = false;
                // CSON
                if (vals.isNullable())
                {
                    aIsNull = vals.isNull(a);
                    bIsNull = vals.isNull(b);
                }
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

                int c;
                switch (type.getType())
                {
                    case Boolean:
                        c = Boolean.compare(vals.getBoolean(a), vals.getBoolean(b));
                        break;
                    case Double:
                        c = Double.compare(vals.getDouble(a), vals.getDouble(b));
                        break;
                    case Float:
                        c = Float.compare(vals.getFloat(a), vals.getFloat(b));
                        break;
                    case Int:
                        c = Integer.compare(vals.getInt(a), vals.getInt(b));
                        break;
                    case Long:
                        c = Long.compare(vals.getLong(a), vals.getLong(b));
                        break;
                    case String:
                        UTF8String aref = vals.getString(a);
                        UTF8String bref = vals.getString(b);
                        c = aref.compareTo(bref);
                        break;
                    case DateTime:
                        c = vals.getDateTime(a)
                                .compareTo(vals.getDateTime(b));
                        break;
                    default:
                        Object aval = vals.getValue(a);
                        Object bval = vals.getValue(b);
                        c = ExpressionMath.cmp(aval, bval);
                        break;
                }

                Order order = sortItem.getOrder();
                if (c != 0)
                {
                    return c * (order == Order.DESC ? -1
                            : 1);
                }
            }
            return 0;
        });

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
                return new ValueVectorAdapter(all.getColumn(column))
                {
                    @Override
                    protected int getRow(int row)
                    {
                        return sortIndices[row];
                    }
                };
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
