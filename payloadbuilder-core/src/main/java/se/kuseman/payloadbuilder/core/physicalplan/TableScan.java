package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.core.QueryException;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;
import se.kuseman.payloadbuilder.core.execution.StatementContext;

/** Scan operator that scans datasource */
public class TableScan implements IPhysicalPlan
{
    protected final int nodeId;
    protected final TableSourceReference tableSource;
    /** The actual catalog name resolved during compile time */
    private final String catalogName;
    protected final IDatasource datasource;
    protected final Schema schema;
    protected final List<Option> options;
    protected final boolean asteriskSchema;

    public TableScan(int nodeId, Schema schema, TableSourceReference tableSource, String catalogName, IDatasource datasource, List<Option> options)
    {
        this.nodeId = nodeId;
        this.schema = requireNonNull(schema, "schema");
        this.tableSource = requireNonNull(tableSource, "tableSource");
        this.catalogName = requireNonNull(catalogName, "catalogName");
        this.datasource = requireNonNull(datasource, "datasource");
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
        return "Scan: " + tableSource.getName();
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
        final StatementContext statementContext = (StatementContext) context.getStatementContext();
        final TupleIterator iterator = datasource.execute(context);
        // Clear any seek keys after we executed the data source to make sure they are not misused
        statementContext.setIndexSeekKeys(tableSource.getId(), null);
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
                statementContext.setRuntimeSchema(tableSource.getId(), vectorSchema);
                validate(vectorSchema, next.getRowCount());
                // If asterisk schema then recreate the schema and attach a table source to make resolved columns properly detect it
                // if not use the planned schema which already has table source attached
                final Schema actualSchema = asteriskSchema ? TableScan.recreateSchema(tableSource, vectorSchema)
                        : schema;
                return new TupleVector()
                {
                    @Override
                    public Schema getSchema()
                    {
                        return actualSchema;
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

    protected void validate(Schema vectorSchema, int rowCount)
    {
        if (!asteriskSchema
                && rowCount > 0)
        {
            if (!schemaEqualsRegardingTypeAndName(schema, vectorSchema))
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
                                         + "Make sure to use the schema returned from Catalog#getTableSchema during planning.");
            }
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

    static boolean schemaEqualsRegardingTypeAndName(Schema expected, Schema actual)
    {
        int size = expected.getSize();
        if (size != actual.getSize())
        {
            return false;
        }
        else
        {
            for (int i = 0; i < size; i++)
            {
                Column schemaColumn = expected.getColumns()
                        .get(i);

                // Don't validate any column
                if (schemaColumn.getType()
                        .getType() == Type.Any)
                {
                    continue;
                }

                Column vectorColumn = actual.getColumns()
                        .get(i);

                if ((schemaColumn.getType()
                        .getType() != vectorColumn.getType()
                                .getType())
                        || !schemaColumn.getName()
                                .equals(vectorColumn.getName()))
                {
                    return false;
                }

                Schema schemaSchema = schemaColumn.getType()
                        .getSchema();
                if (schemaSchema == null)
                {
                    continue;
                }

                // Dig down into nested schemas
                if (!schemaEqualsRegardingTypeAndName(schemaSchema, vectorColumn.getType()
                        .getSchema()))
                {
                    return false;
                }
            }
        }
        return true;
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
            columns.add(CoreColumn.changeTableSource(c, tableSource));
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
                    && datasource.equals(that.datasource);
        }
        return false;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("Scan (").append(nodeId)
                .append("): ");
        sb.append(tableSource.toString());
        return sb.toString();
    }
}
