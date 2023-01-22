package se.kuseman.payloadbuilder.core.catalog.system;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVectorAdapter;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.utils.VectorUtils;

/** Turns a json string into object */
class JsonValueFunction extends ScalarFunctionInfo
{
    private static final ObjectReader READER = new ObjectMapper().readerFor(Object.class);

    JsonValueFunction(Catalog catalog)
    {
        super(catalog, "json_value", FunctionType.SCALAR);
    }

    @Override
    public ResolvedType getType(List<? extends IExpression> arguments)
    {
        return ResolvedType.of(Type.Any);
    }

    @Override
    public int arity()
    {
        return 1;
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<? extends IExpression> arguments)
    {
        final ValueVector value = arguments.get(0)
                .eval(input, context);
        return new ValueVectorAdapter(value)
        {
            @Override
            public ResolvedType type()
            {
                return ResolvedType.of(Type.Any);
            }

            @Override
            public boolean isNullable()
            {
                return value.isNullable();
            }

            @Override
            public boolean isNull(int row)
            {
                return value.isNull(row);
            }

            @Override
            public Object getValue(int row)
            {
                String arg = value.getString(row)
                        .toString();

                try
                {
                    Object obj = READER.readValue(arg);
                    if (obj instanceof Collection
                            || obj.getClass()
                                    .isArray())
                    {
                        return VectorUtils.convertToValueVector(obj);
                    }
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
