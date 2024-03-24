package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Objects.requireNonNull;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;

/** Seek operator that seeks a datasource with a set of seek keys */
public class IndexSeek extends TableScan
{
    private final ISeekPredicate seekPredicate;

    public IndexSeek(int nodeId, Schema schema, TableSourceReference tableSource, String catalogAlias, boolean tempTable, ISeekPredicate seekPredicate, IDatasource datasource, List<Option> options)
    {
        super(nodeId, schema, tableSource, catalogAlias, tempTable, datasource, options);
        this.seekPredicate = requireNonNull(seekPredicate, "seekPredicate");
    }

    @Override
    public String getName()
    {
        return "Index Seek: " + (tempTable ? "#"
                : "")
               + seekPredicate.getIndex();
    }

    @Override
    public Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        Map<String, Object> properties = new LinkedHashMap<>(super.getDescribeProperties(context));
        properties.put(IDatasource.INDEX, seekPredicate.getIndexColumns());
        properties.put("Seek Keys", seekPredicate.toString());
        return properties;
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
        else if (obj instanceof IndexSeek that)
        {
            return super.equals(obj)
                    && seekPredicate.equals(that.seekPredicate);
        }
        return false;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("Index Seek (").append(nodeId)
                .append("): ");
        sb.append(tableSource.toString());
        sb.append(" (keys: ")
                .append(seekPredicate)
                .append(")");
        return sb.toString();
    }
}
