package se.kuseman.payloadbuilder.core.catalog.system;

import java.time.Duration;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.FunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.MutableValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Scalar function that parses a duration string and returns millis. */
class ParseDurationFunction extends ScalarFunctionInfo
{
    public ParseDurationFunction()
    {
        super("parseduration", FunctionInfo.FunctionType.SCALAR);
    }

    @Override
    public Arity arity()
    {
        return Arity.ONE;
    }

    @Override
    public ResolvedType getType(List<IExpression> arguments)
    {
        return ResolvedType.of(Type.Long);
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        return evalScalar(context, input, ValueVector.range(0, input.getRowCount()), catalogAlias, arguments);
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, ValueVector selection, String catalogAlias, List<IExpression> arguments)
    {
        ValueVector value = arguments.get(0)
                .eval(input, selection, context);
        int size = selection.size();
        MutableValueVector result = context.getVectorFactory()
                .getMutableVector(ResolvedType.of(Type.Long), size);

        for (int i = 0; i < size; i++)
        {
            if (value.isNull(i))
            {
                result.setNull(i);
                continue;
            }

            String string = value.getString(i)
                    .toString();
            // If not starting with P we assume HH::mm:ss format, transform into ISO PnDTnHnMn.nS format
            if (string.charAt(0) != 'P')
            {
                String[] split = string.split(":");
                String hours = split[0];
                String minutes = split[1];
                String seconds = split[2];
                string = "PT" + hours + "H" + minutes + "M" + seconds + "S";
            }

            result.setLong(i, Duration.parse(string)
                    .toMillis());
        }

        return result;
    }
}
