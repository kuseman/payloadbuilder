package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.OperatorFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn;
import se.kuseman.payloadbuilder.core.common.DescribableNode;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;

/** Operator function scan that transforms input according to a {@link OperatorFunctionInfo}. */
public class OperatorFunctionScan implements IPhysicalPlan
{
    private final int nodeId;
    private final IPhysicalPlan input;
    private final OperatorFunctionInfo function;
    private final Schema schema;
    private final String catalogAlias;
    private final boolean hasAsteriskSchemaOrInput;

    public OperatorFunctionScan(int nodeId, IPhysicalPlan input, OperatorFunctionInfo function, String catalogAlias, Schema schema)
    {
        this.nodeId = nodeId;
        this.input = requireNonNull(input, "input");
        this.function = requireNonNull(function, "function");
        this.catalogAlias = requireNonNull(catalogAlias, "catalogAlias");
        this.schema = requireNonNull(schema, "schema");
        this.hasAsteriskSchemaOrInput = SchemaUtils.isAsterisk(schema, true);
        if (schema.getColumns()
                .size() != 1)
        {
            throw new IllegalArgumentException("A user defined function should only output one column.");
        }
    }

    @Override
    public int getNodeId()
    {
        return nodeId;
    }

    @Override
    public Schema getSchema()
    {
        return schema;
    }

    @Override
    public Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put(IDatasource.OUTPUT, DescribeUtils.getOutputColumns(getSchema()));
        return properties;
    }

    @Override
    public TupleIterator execute(IExecutionContext context)
    {
        final TupleVector inputVector = PlanUtils.concat(context, input.execute(context));
        if (inputVector.getRowCount() == 0)
        {
            return TupleIterator.EMPTY;
        }

        ValueVector vv = function.eval(context, catalogAlias, inputVector);
        Schema schema;
        // Recreate the schema from input if planed one was asterisk
        if (hasAsteriskSchemaOrInput)
        {
            schema = Schema.of(CoreColumn.Builder.from(this.schema.getColumns()
                    .get(0))
                    .withResolvedType(vv.type())
                    .build());
        }
        else
        {
            schema = this.schema;
        }
        return TupleIterator.singleton(TupleVector.of(schema, vv));

    }

    @Override
    public List<IPhysicalPlan> getChildren()
    {
        return singletonList(input);
    }

    @Override
    public List<DescribableNode> getChildNodes()
    {
        return singletonList(input);
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
        else if (obj instanceof OperatorFunctionScan that)
        {
            return input.equals(that.input)
                    && nodeId == that.nodeId
                    && catalogAlias.equals(that.catalogAlias)
                    && function.equals(that.function)
                    && schema.equals(that.schema);
        }
        return false;
    }

    @Override
    public String toString()
    {
        //@formatter:off
        return "Operator function scan (" + nodeId
               + "): "
               + catalogAlias
               + "#"
               + function.getName()
               + " ("
               + schema
               + ")";
        //@formatter:on
    }
}
