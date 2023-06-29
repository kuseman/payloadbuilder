package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.expression.AsteriskExpression;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

/** Count aggregate. Count all non null inputs */
class AggregateCountFunction extends ScalarFunctionInfo
{
    AggregateCountFunction()
    {
        super("count", FunctionType.SCALAR_AGGREGATE);
    }

    @Override
    public ResolvedType getType(List<IExpression> arguments)
    {
        return ResolvedType.of(Type.Int);
    }

    @Override
    public ResolvedType getAggregateType(List<IExpression> arguments)
    {
        return ResolvedType.of(Type.Int);
    }

    @Override
    public Arity arity()
    {
        return Arity.ONE;
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        final ValueVector vv = arguments.get(0)
                .eval(input, context);
        final Type type = vv.type()
                .getType();
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
                return false;
            }

            @Override
            public int getInt(int row)
            {
                if (type == Type.Array)
                {
                    return count(vv.getArray(row));
                }

                return vv.isNull(row) ? 0
                        : 1;
            }
        };
    }

    @Override
    public ValueVector evalAggregate(IExecutionContext context, AggregateMode mode, ValueVector groups, String catalogAlias, List<IExpression> arguments)
    {
        if (mode == AggregateMode.DISTINCT)
        {
            throw new UnsupportedOperationException(getName() + " DISTINCT is unsupported");
        }

        if (groups.type()
                .getType() != Type.Table)
        {
            throw new IllegalArgumentException("Wrong type of input vector, expected tuple vector but got: " + groups.type());
        }

        IExpression expression = arguments.get(0);
        int size = groups.size();
        final IntList result = new IntArrayList();

        for (int i = 0; i < size; i++)
        {
            // Short cut, no need to evaluate and handle nulls here, just add the input count
            if (expression instanceof AsteriskExpression
                    || expression.isConstant())
            {
                result.add(groups.getTable(i)
                        .getRowCount());
                continue;
            }

            ValueVector vv = expression.eval(groups.getTable(i), context);
            result.add(count(vv));
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
                return result.size();
            }

            @Override
            public boolean isNull(int row)
            {
                return false;
            }

            @Override
            public int getInt(int row)
            {
                return result.getInt(row);
            }
        };
    }

    private int count(ValueVector vector)
    {
        int rowCount = vector.size();
        int count = 0;
        for (int j = 0; j < rowCount; j++)
        {
            if (!vector.isNull(j))
            {
                count++;
            }
        }
        return count;
    }

    @Override
    public String toString()
    {
        return "COUNT";
    }
}
