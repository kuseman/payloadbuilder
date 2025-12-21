package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo.FunctionData;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.QueryException;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;
import se.kuseman.payloadbuilder.core.execution.StatementContext;

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
    public <T, C> T accept(IPhysicalPlanVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put(IDatasource.CATALOG, catalogName);
        properties.put(IDatasource.OUTPUT, DescribeUtils.getOutputColumns(getSchema()));
        properties.putAll(functionInfo.getDescribeProperties(context, catalogAlias, arguments, new FunctionData(nodeId, options)));
        return properties;
    }

    public List<IExpression> getArguments()
    {
        return arguments;
    }

    @Override
    public TupleIterator execute(IExecutionContext context)
    {
        final int batchSize = context.getBatchSize(options);
        final StatementContext statementContext = (StatementContext) context.getStatementContext();
        final TupleIterator iterator = functionInfo.execute(context, catalogAlias, arguments, new FunctionData(nodeId, options));
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
                statementContext.setRuntimeSchema(tableSource.getId(), vectorSchema);
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
    public Schema getSchema()
    {
        return schema;
    }

    @Override
    public List<IPhysicalPlan> getChildren()
    {
        return emptyList();
    }

    private void validate(IExecutionContext context, Schema vectorSchema, int rowCount)
    {
        if (!asteriskSchema
                && rowCount > 0)
        {
            if (!TableScan.schemaEqualsRegardingTypeAndName(schema, vectorSchema))
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
                                         + "Make sure to use the schema returned from TableFunctionInfo#getSchema during planning.");
            }
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
