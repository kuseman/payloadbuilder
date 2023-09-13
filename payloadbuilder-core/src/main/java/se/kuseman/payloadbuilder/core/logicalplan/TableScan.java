package se.kuseman.payloadbuilder.core.logicalplan;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

import java.util.List;

import org.apache.commons.lang3.ObjectUtils;

import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableSchema;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.parser.Location;

/** A table component in the logical plan */
public class TableScan extends TableSource
{
    private final TableSourceReference tableSource;
    /** List of projected columns */
    private final List<String> projection;
    private final TableSchema tableSchema;
    private final boolean tempTable;
    private final List<Option> options;
    private final Location location;

    public TableScan(TableSchema tableSchema, TableSourceReference tableSource, List<String> projection, boolean tempTable, List<Option> options, Location location)
    {
        super(requireNonNull(tableSource, "tableSource").getCatalogAlias(), tableSource.getAlias());
        this.tableSchema = requireNonNull(tableSchema, "tableSchema");
        this.tableSource = tableSource;
        this.projection = ObjectUtils.defaultIfNull(projection, emptyList());
        this.tempTable = tempTable;
        this.options = requireNonNull(options, "options");
        this.location = location;
    }

    public TableSourceReference getTableSource()
    {
        return tableSource;
    }

    public List<String> getProjection()
    {
        return projection;
    }

    public List<Option> getOptions()
    {
        return options;
    }

    public Location getLocation()
    {
        return location;
    }

    public TableSchema getTableSchema()
    {
        return tableSchema;
    }

    public boolean isTempTable()
    {
        return tempTable;
    }

    @Override
    public Schema getSchema()
    {
        return tableSchema.getSchema();
    }

    @Override
    public List<ILogicalPlan> getChildren()
    {
        return emptyList();
    }

    @Override
    public <T, C> T accept(ILogicalPlanVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public int hashCode()
    {
        return tableSource.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }
        else if (obj == null)
        {
            return false;
        }
        else if (obj instanceof TableScan)
        {
            TableScan that = (TableScan) obj;
            return super.equals(that)
                    && tableSchema.equals(that.tableSchema)
                    && tableSource.equals(that.tableSource)
                    && projection.equals(that.projection)
                    && tempTable == that.tempTable
                    && options.equals(that.options);

        }
        return false;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("Scan: ");
        if (tempTable)
        {
            sb.append("#");
        }
        sb.append(tableSource.toString());
        sb.append(", projection: ")
                .append(projection);
        return sb.toString();
    }
}
