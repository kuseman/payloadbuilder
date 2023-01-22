package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Map;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TupleIterator;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.utils.MapUtils;
import se.kuseman.payloadbuilder.core.execution.StatementContext;

/** Scan operator that scans the context outer tuple. Used be nested loops */
public class OverScan implements IPhysicalPlan
{
    private final int nodeId;
    /** Column ordinal used by operators that supports over operator that iterates over a {@link PopulatedValueVector} */
    private final int overOrdinal;
    /** Over alias */
    private final String overAlias;
    private final Schema schema;

    public OverScan(int nodeId, int overOrdinal, String alias, Schema schema)
    {
        this.nodeId = nodeId;
        this.overOrdinal = overOrdinal;
        this.schema = requireNonNull(schema, "schema");
        this.overAlias = requireNonNull(alias, "alias");
    }

    @Override
    public int getNodeId()
    {
        return nodeId;
    }

    @Override
    public String getName()
    {
        return "Over Scan";
    }

    @Override
    public Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        return MapUtils.ofEntries(MapUtils.entry(IDatasource.OUTPUT, DescribeUtils.getOutputColumns(schema)));
    }

    @Override
    public Schema getSchema()
    {
        return schema;
    }

    @Override
    public TupleIterator execute(IExecutionContext context)
    {
        TupleVector outer = ((StatementContext) context.getStatementContext()).getOuterTupleVector();

        if (outer.getRowCount() > 1)
        {
            throw new IllegalArgumentException("Outer tuple should not have more than one row. Alias: " + overAlias);
        }

        ValueVector vv = null;

        if (overOrdinal >= 0)
        {
            vv = outer.getColumn(overOrdinal);
        }
        else
        {
            Column c = outer.getSchema()
                    .getColumn(overAlias);
            if (c != null)
            {
                vv = outer.getColumn(outer.getSchema()
                        .getColumns()
                        .indexOf(c));
            }
        }

        if (vv == null
                || vv.type()
                        .getType() != Type.TupleVector)
        {
            throw new IllegalArgumentException("Over scan require the over alias to be of tuple vector type.");
        }

        if (vv.isNull(0))
        {
            return TupleIterator.EMPTY;
        }

        Object value = vv.getValue(0);
        return TupleIterator.singleton((TupleVector) value);
    }

    @Override
    public List<IPhysicalPlan> getChildren()
    {
        return emptyList();
    }

    @Override
    public int hashCode()
    {
        return overAlias.hashCode();
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
        else if (obj instanceof OverScan)
        {
            OverScan that = (OverScan) obj;
            return nodeId == that.nodeId
                    && overAlias.equals(that.overAlias)
                    && overOrdinal == that.overOrdinal;
        }

        return false;
    }

    @Override
    public String toString()
    {
        return "Over scan (" + nodeId + "): alias:" + overAlias;
    }
}
