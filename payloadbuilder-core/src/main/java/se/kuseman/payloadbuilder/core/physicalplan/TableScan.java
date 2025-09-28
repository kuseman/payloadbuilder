package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.DatasourceData;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.core.QueryException;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;

/** Scan operator that scans datasource */
public class TableScan implements IPhysicalPlan
{
    protected final int nodeId;
    protected final TableSourceReference tableSource;
    /** The actual catalog name resolved during compile time */
    private final String catalogName;
    protected final IDatasource datasource;
    private final Schema schema;
    protected final boolean tempTable;
    protected final List<Option> options;
    protected final boolean asteriskSchema;

    public TableScan(int nodeId, Schema schema, TableSourceReference tableSource, String catalogName, boolean tempTable, IDatasource datasource, List<Option> options)
    {
        this.nodeId = nodeId;
        this.schema = requireNonNull(schema, "schema");
        this.tableSource = requireNonNull(tableSource, "tableSource");
        this.catalogName = requireNonNull(catalogName, "catalogName");
        this.datasource = requireNonNull(datasource, "datasource");
        this.tempTable = tempTable;
        this.options = requireNonNull(options, "options");
        this.asteriskSchema = SchemaUtils.isAsterisk(schema);
    }

    @Override
    public int getNodeId()
    {
        return nodeId;
    }

    @Override
    public String getName()
    {
        return "Scan: " + (tempTable ? "#"
                : "")
               + tableSource.getName();
    }

    @Override
    public Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put(IDatasource.CATALOG, catalogName);
        properties.put(IDatasource.OUTPUT, DescribeUtils.getOutputColumns(schema));
        properties.putAll(datasource.getDescribeProperties(context));
        return properties;
    }

    @Override
    public Schema getSchema()
    {
        return schema;
    }

    @Override
    public TupleIterator execute(IExecutionContext context)
    {
        final int batchSize = context.getBatchSize(options);
        final TupleIterator iterator = datasource.execute(context);
        return new TupleIterator()
        {
            @Override
            public int estimatedBatchCount()
            {
                return iterator.estimatedBatchCount();
            }

            @Override
            public int estimatedRowCount()
            {
                return iterator.estimatedRowCount();
            }

            @Override
            public TupleVector next()
            {
                if (!iterator.hasNext())
                {
                    throw new NoSuchElementException();
                }

                // Concat the data source up to batch size, this might happen if catalog don't implement batch size correct
                final TupleVector next = PlanUtils.concat(context, iterator, batchSize);
                Schema vectorSchema = next.getSchema();
                validate(context, vectorSchema, next.getRowCount());
                if (!asteriskSchema)
                {
                    return next;
                }

                // Attach table source to all asterisk columns in the vector to make column evaluation work properly
                final Schema schema = recreateSchema(tableSource, vectorSchema);
                return new TupleVector()
                {
                    @Override
                    public Schema getSchema()
                    {
                        return schema;
                    }

                    @Override
                    public int getRowCount()
                    {
                        return next.getRowCount();
                    }

                    @Override
                    public ValueVector getColumn(int column)
                    {
                        return next.getColumn(column);
                    }
                };
            }

            @Override
            public boolean hasNext()
            {
                return iterator.hasNext();
            }

            @Override
            public void close()
            {
                iterator.close();
            }
        };
    }

    @Override
    public List<IPhysicalPlan> getChildren()
    {
        return emptyList();
    }

    protected void validate(IExecutionContext context, Schema vectorSchema, int rowCount)
    {
        if (!asteriskSchema
                && rowCount > 0
                && !schema.equals(vectorSchema))
        {
            throw new QueryException("Schema for table: '" + tableSource
                                     + "' doesn't match the planned schema. Check implementation of Catalog: "
                                     + catalogName
                                     + System.lineSeparator()
                                     + "Expected: "
                                     + schema
                                     + System.lineSeparator()
                                     + "Actual: "
                                     + vectorSchema
                                     + System.lineSeparator()
                                     + "Make sure to use the schema provided in '"
                                     + DatasourceData.class.getSimpleName()
                                     + "' when data source is created.");
        }
        else if (asteriskSchema
                && vectorSchema.getSize() <= 0
                && rowCount > 0)
        {
            throw new QueryException("Vector for table: '" + tableSource
                                     + "' returned an empty schema. Check implementation of Catalog: "
                                     + catalogName
                                     + System.lineSeparator()
                                     + "Make sure to provide the actual runtime schema of the vector when using an asterisk schema.");
        }
    }

    static Schema recreateSchema(TableSourceReference tableSource, Schema schema)
    {
        int size = schema.getSize();
        List<Column> columns = new ArrayList<>(size);
        for (int i = 0; i < size; i++)
        {
            Column c = schema.getColumns()
                    .get(i);
            if (SchemaUtils.isAsterisk(c))
            {
                throw new QueryException("Runtime tuple vectors cannot contain asterisk columns");
            }
            columns.add(SchemaUtils.changeTableSource(c, tableSource));
        }

        return new Schema(columns);
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
        else if (obj instanceof TableScan)
        {
            TableScan that = (TableScan) obj;
            return nodeId == that.nodeId
                    && schema.equals(that.schema)
                    && tableSource.equals(that.tableSource)
                    && catalogName.equals(that.catalogName)
                    && datasource.equals(that.datasource)
                    && tempTable == that.tempTable;
        }
        return false;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("Scan (").append(nodeId)
                .append("): ");
        if (tempTable)
        {
            sb.append("#");
        }
        sb.append(tableSource.toString());
        return sb.toString();
    }
}
