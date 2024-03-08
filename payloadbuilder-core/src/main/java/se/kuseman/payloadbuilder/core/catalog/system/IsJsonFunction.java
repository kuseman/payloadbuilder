package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Checks if provided value is vaild json */
class IsJsonFunction extends ScalarFunctionInfo
{
    static ObjectMapper MAPPER = new ObjectMapper();
    static final ObjectReader READER = MAPPER.readerFor(Object.class);
    static final ObjectReader OBJECT_READER = MAPPER.readerFor(new TypeReference<Map<String, Object>>()
    {
    });
    static final ObjectReader ARRAY_READER = new ObjectMapper().readerFor(List.class);

    private final ObjectReader reader;

    private IsJsonFunction(String name, ObjectReader reader)
    {
        super(name, FunctionType.SCALAR);
        this.reader = requireNonNull(reader, "reader");
    }

    @Override
    public ResolvedType getType(List<IExpression> arguments)
    {
        return ResolvedType.of(Type.Boolean);
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
        return new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                return ResolvedType.of(Type.Boolean);
            }

            @Override
            public int size()
            {
                return input.getRowCount();
            }

            @Override
            public boolean isNull(int row)
            {
                return value.isNull(row);
            }

            @Override
            public boolean getBoolean(int row)
            {
                UTF8String string = value.getString(row);
                if (UTF8String.EMPTY.equals(string))
                {
                    return false;
                }

                try
                {
                    reader.readValue(string.toString());
                    return true;
                }
                catch (Exception e)
                {
                    return false;
                }
            }
        };
    }

    static ScalarFunctionInfo isJson()
    {
        return new IsJsonFunction("isjson", READER);
    }

    static ScalarFunctionInfo isJsonObject()
    {
        return new IsJsonFunction("isjsonobject", OBJECT_READER);
    }

    static ScalarFunctionInfo isJsonArray()
    {
        return new IsJsonFunction("isjsonarray", ARRAY_READER);
    }
}
