package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.defaultIfBlank;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.api.catalog.TupleIterator;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.QuerySession;
import se.kuseman.payloadbuilder.core.common.Option;

/** A table component in the logical plan */
public class TableFunctionScan implements IPhysicalPlan
{
    private final int nodeId;
    private final TableSourceReference tableSource;
    private final String catalogAlias;
    private final TableFunctionInfo functionInfo;
    private final List<IExpression> arguments;
    private final List<Option> options;

    public TableFunctionScan(int nodeId, TableSourceReference tableSource, String catalogAlias, TableFunctionInfo functionInfo, List<IExpression> arguments, List<Option> options)
    {
        this.nodeId = nodeId;
        this.tableSource = requireNonNull(tableSource, "tableSource");
        this.catalogAlias = requireNonNull(catalogAlias, "catalogAlias");
        this.functionInfo = requireNonNull(functionInfo, "functionInfo");
        this.arguments = requireNonNull(arguments, "arguments");
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
        return "Function Scan: " + tableSource.getName()
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
        properties.put(IDatasource.OUTPUT, DescribeUtils.getOutputColumns(functionInfo.getSchema(arguments)));
        return properties;
    }

    public List<IExpression> getArguments()
    {
        return arguments;
    }

    @Override
    public TupleIterator execute(IExecutionContext context)
    {
        final TupleIterator iterator = functionInfo.execute(context, catalogAlias, arguments, new DatasourceOptions(options));
        return new TupleIterator()
        {
            @Override
            public TupleVector next()
            {
                final TupleVector next = iterator.next();
                // Recreate the schema and attach a table source to make resolved column properly detect it
                final Schema schema = new Schema(next.getSchema()
                        .getColumns()
                        .stream()
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
    public Schema getSchema()
    {
        return functionInfo.getSchema(arguments);
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
        if (obj instanceof TableFunctionScan)
        {
            TableFunctionScan that = (TableFunctionScan) obj;
            return nodeId == that.nodeId
                    && catalogAlias.equals(that.catalogAlias)
                    && functionInfo.equals(that.functionInfo)
                    && arguments.equals(that.arguments);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "Function scan: " + functionInfo.getCatalog()
                .getName()
               + "#"
               + functionInfo.getName()
               + "("
               + arguments.stream()
                       .map(IExpression::toVerboseString)
                       .collect(joining(", "))
               + ")";
    }
}
