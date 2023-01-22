package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

/** Count aggregate. Count all non null inputs */
class AggregateCountFunction extends ScalarFunctionInfo
{
    AggregateCountFunction(Catalog catalog)
    {
        super(catalog, "count", FunctionType.SCALAR_AGGREGATE);
    }

    @Override
    public ResolvedType getType(List<? extends IExpression> arguments)
    {
        return ResolvedType.of(Type.Int);
    }

    @Override
    public int arity()
    {
        return 1;
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<? extends IExpression> arguments)
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
            public boolean isNullable()
            {
                return false;
            }

            @Override
            public boolean isNull(int row)
            {
                return false;
            }

            @Override
            public int getInt(int row)
            {
                if (type == Type.ValueVector)
                {
                    return count((ValueVector) vv.getValue(row));
                }

                boolean isNull = vv.isNullable()
                        && vv.isNull(row);
                return isNull ? 0
                        : 1;
            }

            @Override
            public Object getValue(int row)
            {
                throw new IllegalArgumentException("getValue should not be called");
            }
        };
    }

    @Override
    public ValueVector evalAggregate(IExecutionContext context, AggregateMode mode, ValueVector groups, String catalogAlias, List<? extends IExpression> arguments)
    {
        if (mode == AggregateMode.DISTINCT)
        {
            throw new UnsupportedOperationException(getName() + " DISTINCT is unsupported");
        }

        if (groups.type()
                .getType() != Type.TupleVector)
        {
            throw new IllegalArgumentException("Wrong type of input vector, expected tuple vector but got: " + groups.type());
        }

        IExpression expression = arguments.get(0);
        int size = groups.size();
        final IntList result = new IntArrayList();

        for (int i = 0; i < size; i++)
        {
            ValueVector vv = expression.eval((TupleVector) groups.getValue(i), context);
            // Replace the value vector we're counting to the inner
            if (vv.type()
                    .getType() == Type.ValueVector)
            {
                int count = vv.size();
                for (int j = 0; j < count; j++)
                {
                    result.add(count((ValueVector) vv.getValue(j)));
                }
            }
            else
            {
                result.add(count(vv));
            }
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
            public boolean isNullable()
            {
                return false;
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

            @Override
            public Object getValue(int row)
            {
                throw new IllegalArgumentException("getValue should not be called");
            }
        };
    }

    private int count(ValueVector vector)
    {
        int rowCount = vector.size();
        // TODO: asterisk count flag => count all and ignore nulls
        // Not nullable then we can simply pick the vector size
        if (!vector.isNullable())
        {
            return rowCount;
        }

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
