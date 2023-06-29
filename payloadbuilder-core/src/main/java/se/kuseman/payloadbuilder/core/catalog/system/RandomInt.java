package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;
import java.util.Random;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Function random int, returns a random int from provided seed. */
class RandomInt extends ScalarFunctionInfo
{
    private final Random random;

    RandomInt()
    {
        super("randomInt", FunctionType.SCALAR);
        this.random = new Random(System.nanoTime());
    }

    @Override
    public Arity arity()
    {
        return Arity.ONE;
    }

    @Override
    public ResolvedType getType(List<IExpression> arguments)
    {
        return ResolvedType.of(Type.Int);
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        final ValueVector bound = arguments.get(0)
                .eval(input, context);
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
                return bound.isNull(row);
            }

            @Override
            public int getInt(int row)
            {
                // Might be weird to get different values on each invocation
                // and also for each row
                return random.nextInt(bound.getInt(row));
            }
        };
    }
}
