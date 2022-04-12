package se.kuseman.payloadbuilder.core.operator;

import static java.util.Objects.requireNonNull;

import java.util.Arrays;
import java.util.Map;

import se.kuseman.payloadbuilder.api.OutputWriter;
import se.kuseman.payloadbuilder.api.codegen.CodeGeneratorContext;
import se.kuseman.payloadbuilder.api.codegen.ProjectionCode;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.api.operator.Tuple;
import se.kuseman.payloadbuilder.api.utils.MapUtils;

/** Projection for asterisk selects */
public class AsteriskProjection extends AProjection
{
    private final int[] tupleOrdinals;

    public AsteriskProjection(int[] tupleOrdinals)
    {
        this.tupleOrdinals = requireNonNull(tupleOrdinals);
    }

    @Override
    public String getName()
    {
        return "Asterisk";
    }

    @Override
    public Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        return MapUtils.ofEntries(MapUtils.entry("Ordinals", tupleOrdinals));
    }

    @Override
    public boolean isAsterisk()
    {
        return true;
    }

    @Override
    public ProjectionCode generateCode(CodeGeneratorContext context)
    {
        int index = context.addReference(this);
        ProjectionCode code = context.getProjectionCode();
        code.setCode("((Projection) references[" + index + "]).writeValue(writer, context);\n");
        return code;
    }

    @Override
    public void writeValue(OutputWriter writer, ExecutionContext context)
    {
        Tuple tuple = context.getStatementContext()
                .getTuple();

        int length = tupleOrdinals.length;
        if (length == 0)
        {
            writeTupleValues(writer, context, tuple);
        }
        else
        {
            for (int i = 0; i < length; i++)
            {
                int tupleOrdinal = tupleOrdinals[i];
                writeTupleValues(writer, context, tuple.getTuple(tupleOrdinal));
            }
        }
    }

    private void writeTupleValues(OutputWriter writer, ExecutionContext context, Tuple tuple)
    {
        if (tuple == null)
        {
            return;
        }
        int count = tuple.getColumnCount();
        for (int i = 0; i < count; i++)
        {
            writer.writeFieldName(tuple.getColumn(i));
            Object value = tuple.getValue(i);
            writeValue(writer, context, value);
        }
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(tupleOrdinals);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof AsteriskProjection)
        {
            AsteriskProjection that = (AsteriskProjection) obj;
            return Arrays.equals(tupleOrdinals, that.tupleOrdinals);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "*";
    }
}
