package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;

import org.apache.commons.lang3.StringUtils;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.ILongVectorBuilder;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Charindex string function */
class CharIndexFunction extends ScalarFunctionInfo
{
    CharIndexFunction()
    {
        super("charindex", FunctionType.SCALAR);
    }

    @Override
    public ResolvedType getType(List<IExpression> arguments)
    {
        return ResolvedType.of(Type.Long);
    }

    @Override
    public Arity arity()
    {
        return new Arity(2, 3);
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        ValueVector find = arguments.get(0)
                .eval(input, context);
        ValueVector search = arguments.get(1)
                .eval(input, context);
        ValueVector start = arguments.size() > 2 ? arguments.get(2)
                .eval(input, context)
                : null;

        int rowCount = input.getRowCount();
        ILongVectorBuilder builder = context.getVectorBuilderFactory()
                .getLongVectorBuilder(rowCount);

        for (int i = 0; i < rowCount; i++)
        {
            if (search.isNull(i)
                    || find.isNull(i))
            {
                builder.putNull();
            }
            else
            {
                int startLocation = 0;
                if (start != null)
                {
                    startLocation = start.getInt(i);
                }

                String string = search.getString(i)
                        .toString();
                String findString = find.getString(i)
                        .toString();

                long index = StringUtils.indexOf(string, findString, startLocation);

                builder.put(index);
            }
        }
        return builder.build();
    }
}
