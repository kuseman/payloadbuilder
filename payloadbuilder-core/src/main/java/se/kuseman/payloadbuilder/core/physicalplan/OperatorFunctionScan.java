package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.OperatorFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TupleIterator;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;

/** Operator function scan that transforms input according to a {@link OperatorFunctionInfo}. */
public class OperatorFunctionScan implements IPhysicalPlan
{
    private final int nodeId;
    private final IPhysicalPlan input;
    private final OperatorFunctionInfo function;
    private final Schema schema;
    private final String catalogAlias;

    public OperatorFunctionScan(int nodeId, IPhysicalPlan input, OperatorFunctionInfo function, String catalogAlias, Schema schema)
    {
        this.nodeId = nodeId;
        this.input = requireNonNull(input, "input");
        this.function = requireNonNull(function, "function");
        this.catalogAlias = requireNonNull(catalogAlias, "catalogAlias");
        this.schema = requireNonNull(schema, "schema");

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
    public TupleIterator execute(IExecutionContext context)
    {
        final TupleVector inputVector = PlanUtils.concat(input.execute(context));
        if (inputVector.getRowCount() == 0)
        {
            return TupleIterator.EMPTY;
        }

        ValueVector vv = function.eval(context, catalogAlias, inputVector);
        TupleVector result = TupleVector.of(schema, singletonList(vv));
        return TupleIterator.singleton(result);
    }

    @Override
    public List<IPhysicalPlan> getChildren()
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
        else if (obj instanceof OperatorFunctionScan)
        {
            OperatorFunctionScan that = (OperatorFunctionScan) obj;
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
        return "Operator function scan (" + nodeId
               + "): "
               + function.getCatalog()
                       .getName()
               + "#"
               + function.getName()
               + " ("
               + schema
               + ")";
    }
}
