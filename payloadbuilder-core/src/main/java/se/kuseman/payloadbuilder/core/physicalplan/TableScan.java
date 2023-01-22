package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.defaultIfBlank;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.api.catalog.TupleIterator;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.core.QueryException;
import se.kuseman.payloadbuilder.core.QuerySession;
import se.kuseman.payloadbuilder.core.common.Option;

/** Scan operator that scans datasource */
public class TableScan implements IPhysicalPlan
{
    protected final int nodeId;
    protected final TableSourceReference tableSource;
    protected final IDatasource datasource;
    private final Schema schema;
    private final boolean tempTable;
    private final List<Option> options;

    public TableScan(int nodeId, Schema schema, TableSourceReference tableSource, boolean tempTable, IDatasource datasource, List<Option> options)
    {
        this.nodeId = nodeId;
        this.schema = requireNonNull(schema, "schema");
        this.tableSource = requireNonNull(tableSource, "tableSource");
        this.datasource = requireNonNull(datasource, "datasource");
        this.tempTable = tempTable;
        this.options = requireNonNull(options, "options");
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
               + tableSource.getName()
                       .toDotDelimited();
    }

    @Override
    public Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        String catalogAlias = defaultIfBlank(tableSource.getCatalogAlias(), ((QuerySession) context.getSession()).getDefaultCatalogAlias());
        Catalog catalog = ((QuerySession) context.getSession()).getCatalogRegistry()
                .getCatalog(catalogAlias);
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put(IDatasource.CATALOG, catalog.getName());
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
        final TupleIterator iterator = datasource.execute(context, new DatasourceOptions(options));
        return new TupleIterator()
        {
            @Override
            public TupleVector next()
            {
                if (!iterator.hasNext())
                {
                    throw new NoSuchElementException();
                }

                final TupleVector next = iterator.next();
                // Recreate the schema and attach a table source to make resolved columns properly detect it
                final Schema schema = new Schema(next.getSchema()
                        .getColumns()
                        .stream()
                        .peek(c ->
                        {
                            if (c.getColumnReference() != null
                                    && c.getColumnReference()
                                            .isAsterisk())
                            {
                                throw new QueryException("Runtime tuple vectors cannot contain asterisk columns");
                            }
                        })
                        .map(c -> new Column(c, tableSource))
                        .collect(toList()));

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
