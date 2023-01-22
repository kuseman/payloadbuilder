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
import se.kuseman.payloadbuilder.api.utils.VectorUtils;

/** Function toList. Creates a list of provided arguments */
class ToListFunction extends ScalarFunctionInfo
{
    ToListFunction(Catalog catalog)
    {
        super(catalog, "tolist", FunctionType.SCALAR);
    }

    @Override
    public String getDescription()
    {
        return "Converts provided argument to a list that payloadbuilder can interpret. Argument can be a list/collection/array. " + System.lineSeparator() + "ie. tolist(a.col)";
    }

    @Override
    public int arity()
    {
        return 1;
    }

    @Override
    public ResolvedType getType(List<? extends IExpression> arguments)
    {
        return ResolvedType.valueVector(ResolvedType.of(Type.Any));
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<? extends IExpression> arguments)
    {
        final ValueVector result = arguments.get(0)
                .eval(input, context);

        return new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                return ResolvedType.valueVector(ResolvedType.of(Type.Any));
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
            public Object getValue(int row)
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
