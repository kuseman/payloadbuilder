package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.IDatasourceOptions;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.QueryException;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;

/** A table component in the logical plan */
public class TableFunctionScan implements IPhysicalPlan
{
    private final int nodeId;
    private final TableSourceReference tableSource;
    private final Schema schema;
    private final String catalogAlias;
    private final String catalogName;
    private final TableFunctionInfo functionInfo;
    private final List<IExpression> arguments;
    private final List<Option> options;
    private final boolean asteriskSchema;

    public TableFunctionScan(int nodeId, Schema schema, TableSourceReference tableSource, String catalogAlias, String catalogName, TableFunctionInfo functionInfo, List<IExpression> arguments,
            List<Option> options)
    {
        this.nodeId = nodeId;
        this.schema = requireNonNull(schema, "schema");
        this.tableSource = requireNonNull(tableSource, "tableSource");
        this.catalogAlias = requireNonNull(catalogAlias, "catalogAlias");
        this.catalogName = requireNonNull(catalogName, "catalogName");
        this.functionInfo = requireNonNull(functionInfo, "functionInfo");
        this.arguments = requireNonNull(arguments, "arguments");
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
        return "Function Scan: " + tableSource.getName();
    }

    @Override
    public Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put(IDatasource.CATALOG, catalogName);
        properties.put(IDatasource.OUTPUT, DescribeUtils.getOutputColumns(getSchema()));
        properties.putAll(functionInfo.getDescribeProperties(context));
        return properties;
    }

    public List<IExpression> getArguments()
    {
        return arguments;
    }

    @Override
    public TupleIterator execute(IExecutionContext context)
    {
        Optional<Schema> schema = asteriskSchema ? Optional.empty()
                : Optional.of(this.schema);
        final IDatasourceOptions datasourceOptions = new DatasourceOptions(options);
        final int batchSize = datasourceOptions.getBatchSize(context);
        final TupleIterator iterator = functionInfo.execute(context, catalogAlias, schema, arguments, datasourceOptions, nodeId);
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
                // Concat the data source up to batch size, this might happen if catalog don't implement batch size correct
                final TupleVector next = PlanUtils.concat(context, iterator, batchSize);
                Schema vectorSchema = next.getSchema();
                validate(context, vectorSchema, next.getRowCount());
                if (!asteriskSchema)
                {
                    return next;
                }

                // Recreate the schema and attach a table source to make resolved columns properly detect it
                final Schema schema = TableScan.recreateSchema(tableSource, next.getSchema());
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
        return schema;
    }

    @Override
    public List<IPhysicalPlan> getChildren()
    {
        return emptyList();
    }

    private boolean validate(IExecutionContext context, Schema vectorSchema, int rowCount)
    {
        if (!asteriskSchema
                && rowCount > 0
                && !schema.equals(vectorSchema))
        {
            throw new QueryException("Schema for function: '" + functionInfo.getName()
                                     + "' doesn't match the planned schema. Check implementation of Catalog: "
                                     + catalogName
                                     + System.lineSeparator()
                                     + "Expected: "
                                     + schema
                                     + System.lineSeparator()
                                     + "Actual: "
                                     + vectorSchema
                                     + System.lineSeparator()
                                     + "Make sure to use the schema provided in execute method.");
        }
        else if (asteriskSchema
                && vectorSchema.getSize() <= 0
                && rowCount > 0)
        {
            throw new QueryException("Vector for function: '" + functionInfo.getName()
                                     + "' returned an empty schema. Check implementation of Catalog: "
                                     + catalogName
                                     + System.lineSeparator()
                                     + "Make sure to provide the actual runtime schema of the vector when using an asterisk schema.");
        }
        return true;
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
                    && catalogName.equals(that.catalogName)
                    && functionInfo.equals(that.functionInfo)
                    && arguments.equals(that.arguments);
        }
        return false;
    }

    @Override
    public String toString()
    {
        //@formatter:off
        return "Function scan: "
               + catalogAlias
               + "#"
               + functionInfo.getName()
               + "("
               + arguments.stream()
                       .map(IExpression::toVerboseString)
                       .collect(joining(", "))
               + ")";
        //@formatter:on
    }
}
