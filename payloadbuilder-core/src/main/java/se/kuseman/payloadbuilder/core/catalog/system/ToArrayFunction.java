package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.execution.VectorUtils;

/** Function toArray. Creates an array of provided arguments */
class ToArrayFunction extends ScalarFunctionInfo
{
    ToArrayFunction()
    {
        super("toarray", FunctionType.SCALAR);
    }

    @Override
    public String getDescription()
    {
        return "Converts provided argument to a array that payloadbuilder can interpret. Argument can be a list/collection/array. " + System.lineSeparator()
               + "ie. toArray(a.col). Behaves the same as CAST(a.col AS Array).";
    }

    @Override
    public Arity arity()
    {
        return Arity.ONE;
    }

    @Override
    public ResolvedType getType(List<IExpression> arguments)
    {
        return ResolvedType.array(ResolvedType.of(Type.Any));
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        final ValueVector result = arguments.get(0)
                .eval(input, context);

        return new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                return ResolvedType.array(ResolvedType.of(Type.Any));
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
            public ValueVector getArray(int row)
            {
                return getValueVector(result.valueAsObject(row));
            }
        };
    }

    /** Returns a nested value vector that is used when we are using a lambda for each value in a known iterable type like lists/maps/arrays etc.. */
    protected static ValueVector getValueVector(Object value)
    {
        if (value == null)
        {
            return ValueVector.literalNull(ResolvedType.of(Type.Any), 1);
        }
        return VectorUtils.convertToValueVector(value);
    }
}
