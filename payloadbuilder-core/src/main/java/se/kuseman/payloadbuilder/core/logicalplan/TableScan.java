package se.kuseman.payloadbuilder.core.logicalplan;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.DatasourceData.Projection;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableSchema;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference.Type;
import se.kuseman.payloadbuilder.core.parser.Location;

/** A table component in the logical plan */
public class TableScan extends TableSource
{
    /** List of projected columns */
    private final Projection projection;
    private final TableSchema tableSchema;
    private final boolean tempTable;
    private final List<Option> options;
    private final Location location;

    public TableScan(TableSchema tableSchema, TableSourceReference tableSource, Projection projection, boolean tempTable, List<Option> options, Location location)
    {
        super(tableSource);
        this.tableSchema = requireNonNull(tableSchema, "tableSchema");
        this.projection = requireNonNull(projection, "projection");
        this.tempTable = tempTable;
        this.options = requireNonNull(options, "options");
        this.location = location;
        if (tableSource.getType() != Type.TABLE)
        {
            throw new IllegalArgumentException("Wrong table source type");
        }

    }

    public Projection getProjection()
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
        else if (obj instanceof TableScan that)
        {
            return super.equals(that)
                    && tableSchema.equals(that.tableSchema)
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
