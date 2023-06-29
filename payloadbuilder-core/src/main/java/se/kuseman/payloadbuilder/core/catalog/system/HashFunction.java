package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.execution.VectorUtils;

/** Hash function. Hahses arguments using Objects.hash */
class HashFunction extends ScalarFunctionInfo
{
    HashFunction()
    {
        super("hash", FunctionType.SCALAR);
    }

    @Override
    public ResolvedType getType(List<IExpression> arguments)
    {
        return ResolvedType.of(Column.Type.Int);
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        int size = arguments.size();
        final ValueVector[] vectors = new ValueVector[size];
        for (int i = 0; i < size; i++)
        {
            vectors[i] = arguments.get(i)
                    .eval(input, context);
        }

        return new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                return ResolvedType.of(Type.Int);
            }

            @Override
            public int size()
            {
                return input.getRowCount();
            }

            @Override
            public boolean isNull(int row)
            {
                for (int i = 0; i < size; i++)
                {
                    if (vectors[i].isNull(row))
                    {
                        return true;
                    }
                }

                return false;
            }

            @Override
            public int getInt(int row)
            {
                return VectorUtils.hash(vectors, row);
            }
        };
    }
}
