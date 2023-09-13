package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Replace */
class ReplaceFunction extends ScalarFunctionInfo
{
    ReplaceFunction()
    {
        super("replace", FunctionType.SCALAR);
    }

    @Override
    public String getDescription()
    {
        return "Replaces all occurrences of specified string with a replacement string" + System.lineSeparator()
               + "Ex. replace(expression, searchExpression, replaceExpression)"
               + System.lineSeparator()
               + "NOTE! All input arguments is converted to String if not String already."
               + System.lineSeparator()
               + "      If any input evaluates to null, null is returned";
    }

    @Override
    public ResolvedType getType(List<IExpression> arguments)
    {
        return ResolvedType.of(Type.String);
    }

    @Override
    public Arity arity()
    {
        return new Arity(3, 3);
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        final ValueVector value = arguments.get(0)
                .eval(input, context);
        final ValueVector search = arguments.get(1)
                .eval(input, context);
        final ValueVector replacement = arguments.get(2)
                .eval(input, context);

        return new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                return ResolvedType.of(Type.String);
            }

            @Override
            public int size()
            {
                return input.getRowCount();
            }

            @Override
            public boolean isNull(int row)
            {
                return value.isNull(row)
                        || search.isNull(row)
                        || replacement.isNull(row);
            }

            @Override
            public UTF8String getString(int row)
            {
                String strValue = value.getString(row)
                        .toString();
                String strSearch = search.getString(row)
                        .toString();
                String strReplacement = replacement.getString(row)
                        .toString();

                return UTF8String.from(strValue.replace(strSearch, strReplacement));
            }
        };
    }
}
