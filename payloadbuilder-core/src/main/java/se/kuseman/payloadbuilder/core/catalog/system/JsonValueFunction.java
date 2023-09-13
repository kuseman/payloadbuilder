package se.kuseman.payloadbuilder.core.catalog.system;

import java.io.IOException;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.execution.ValueVectorAdapter;

/** Turns a json string into object */
class JsonValueFunction extends ScalarFunctionInfo
{
    JsonValueFunction()
    {
        super("json_value", FunctionType.SCALAR);
    }

    @Override
    public ResolvedType getType(List<IExpression> arguments)
    {
        return ResolvedType.of(Type.Any);
    }

    @Override
    public Arity arity()
    {
        return Arity.ONE;
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        final ValueVector value = arguments.get(0)
                .eval(input, context);
        return new ValueVectorAdapter(value)
        {
            int cacheRow = -1;
            String cacheValue;

            @Override
            public ResolvedType type()
            {
                return ResolvedType.of(Type.Any);
            }

            @Override
            public boolean isNull(int row)
            {
                boolean isNull = value.isNull(row);

                // Verify 'null' string that will be null when parsed as json
                if (!isNull)
                {
                    cacheRow = row;
                    cacheValue = value.getString(row)
                            .toString();

                    isNull = "null".equalsIgnoreCase(cacheValue);
                }
                return isNull;
            }

            @Override
            public Object getAny(int row)
            {
                String arg = cacheRow == row ? cacheValue
                        : value.getString(row)
                                .toString();

                try
                {
                    Object obj = IsJsonFunction.READER.readValue(arg);
                    return obj;
                }
                catch (IOException e)
                {
                    throw new IllegalArgumentException("Error deserializing '" + arg + "'", e);
                }
            }
        };
    }
}
