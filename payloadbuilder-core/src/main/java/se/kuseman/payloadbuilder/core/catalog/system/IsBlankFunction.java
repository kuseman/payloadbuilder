package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.UTF8String;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVectorAdapter;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Returns first item that is not a blank string (null or empty) */
class IsBlankFunction extends ScalarFunctionInfo
{
    IsBlankFunction(Catalog catalog)
    {
        super(catalog, "isblank", FunctionType.SCALAR);
    }

    @Override
    public String getDescription()
    {
        return "Returns first non blank value of provided arguments. " + System.lineSeparator()
               + "Ex. isblank(expression1, expression2)"
               + System.lineSeparator()
               + "If both arguments is blank, second argument is returned. "
               + System.lineSeparator()
               + "NOTE! First argument is transfomed to a string to determine blank-ness.";
    }

    @Override
    public int arity()
    {
        return 2;
    }

    @Override
    public ResolvedType getType(List<? extends IExpression> arguments)
    {
        ResolvedType arg0Type = arguments.get(0)
                .getType();
        ResolvedType arg1Type = arguments.get(1)
                .getType();

        return arg0Type.getType()
                .getPrecedence() >= arg1Type.getType()
                        .getPrecedence() ? arg0Type
                                : arg1Type;
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<? extends IExpression> arguments)
    {
        final ValueVector value = arguments.get(0)
                .eval(input, context);
        final ValueVector arg1 = arguments.get(1)
                .eval(input, context);

        boolean nullable = value.isNullable()
                || arg1.isNullable();
        ResolvedType type = getType(arguments);

        return new ValueVectorAdapter(row ->
        {
            if (value.isNull(row)
                    || UTF8String.EMPTY.equals(value.getString(row)))
            {
                return arg1;
            }

            return value;
        }, input.getRowCount(), nullable, type);
    }
}
