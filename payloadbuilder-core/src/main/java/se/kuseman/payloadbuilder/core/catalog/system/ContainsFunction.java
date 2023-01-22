package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;
import java.util.Objects;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Function listOf. Creates a list of provided arguments */
class ContainsFunction extends ScalarFunctionInfo
{
    ContainsFunction(Catalog catalog)
    {
        super(catalog, "contains", FunctionType.SCALAR);
    }

    @Override
    public String getDescription()
    {
        return "Checks if provided array contains value argument" + System.lineSeparator() + "ie. contains(<array expression>, <value expression>)";
    }

    @Override
    public int arity()
    {
        return 2;
    }

    @Override
    public ResolvedType getType(List<? extends IExpression> arguments)
    {
        return ResolvedType.of(Column.Type.Boolean);
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<? extends IExpression> arguments)
    {
        final ValueVector array = arguments.get(0)
                .eval(input, context);
        final ValueVector findValue = arguments.get(1)
                .eval(input, context);

        final Type arrayType = array.type()
                .getType();

        return new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                return ResolvedType.of(Column.Type.Boolean);
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
            public boolean getBoolean(int row)
            {
                Object findVal = findValue.valueAsObject(row);
                Object arrVal = array.valueAsObject(row);

                // Search vector
                if (arrayType == Type.ValueVector)
                {
                    ValueVector vec = (ValueVector) arrVal;
                    int size = vec.size();
                    for (int i = 0; i < size; i++)
                    {
                        if (Objects.equals(vec.valueAsObject(i), findVal))
                        {
                            return true;
                        }
                    }
                    return false;
                }

                return Objects.equals(arrVal, findVal);
            }

            @Override
            public Object getValue(int row)
            {
                throw new IllegalArgumentException("getValue should not be called on typed vectors");
            }
        };
    }
}
