package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.MutableValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Parses a data size string like 1kb, 1mb, 1.4gb into a long value with bytes. */
class ParseDataSizeFunction extends ScalarFunctionInfo
{
    private static final Pattern PATTERN = Pattern.compile("^(-?(0|[1-9]\\d*)(\\.\\d+)?)([a-zA-Z]{0,2})$");
    private static final long KB = 1024;
    private static final long MB = KB * 1024;
    private static final long GB = MB * 1024;
    private static final long TB = GB * 1024;

    ParseDataSizeFunction()
    {
        super("parsedatasize", FunctionType.SCALAR);
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

            Matcher matcher = PATTERN.matcher(string);
            if (!matcher.matches())
            {
                throw new IllegalArgumentException("Could not parse data size: " + string);
            }

            String unit = matcher.group(4);
            float dataSize = Float.valueOf(matcher.group(1));
            if ("b".equalsIgnoreCase(unit))
            {
                result.setLong(i, (long) dataSize);
            }
            else if ("kb".equalsIgnoreCase(unit))
            {
                result.setLong(i, (long) (KB * dataSize));
            }
            else if ("mb".equalsIgnoreCase(unit))
            {
                result.setLong(i, (long) (MB * dataSize));
            }
            else if ("gb".equalsIgnoreCase(unit))
            {
                result.setLong(i, (long) (GB * dataSize));
            }
            else if ("tb".equalsIgnoreCase(unit))
            {
                result.setLong(i, (long) (TB * dataSize));
            }
            else
            {
                throw new IllegalArgumentException("Unkown data size unit: " + unit);
            }
        }
        return result;
    }
}
