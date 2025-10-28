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

/** Function contains. Creates a list of provided arguments */
class ContainsFunction extends ScalarFunctionInfo
{
    ContainsFunction()
    {
        super("contains", FunctionType.SCALAR);
    }

    @Override
    public String getDescription()
    {
        return "Checks if provided array contains value argument" + System.lineSeparator() + "ie. contains(<array expression>, <value expression>)";
    }

    @Override
    public Arity arity()
    {
        return Arity.TWO;
    }

    @Override
    public ResolvedType getType(List<IExpression> arguments)
    {
        return ResolvedType.of(Column.Type.Boolean);
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        final ValueVector array = arguments.get(0)
                .eval(input, context);
        final ValueVector findValue = arguments.get(1)
                .eval(input, context);

        final Type arrayType = array.type()
                .getType();
        final Type equalsType = getEqualsType(array, findValue);

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
            public boolean isNull(int row)
            {
                return false;
            }

            @Override
            public boolean getBoolean(int row)
            {
                // A null array cannot be searched and hence => false
                if (array.isNull(row))
                {
                    return false;
                }

                ValueVector currentArray = null;
                if (arrayType == Type.Any)
                {
                    Object arrayValue = VectorUtils.convertToValueVector(array.valueAsObject(row), false);
                    if (arrayValue instanceof ValueVector vector)
                    {
                        currentArray = vector;
                    }
                }
                else if (arrayType == Type.Array)
                {
                    currentArray = array.getArray(row);
                }

                if (currentArray != null)
                {
                    int size = currentArray.size();
                    for (int i = 0; i < size; i++)
                    {
                        // NOTE! When searching arrays we compare nulls so it's possible to perform a "arr.contains(null)"
                        if (VectorUtils.equals(currentArray, findValue, equalsType, i, row, true))
                        {
                            return true;
                        }
                    }
                    return false;
                }

                return VectorUtils.equals(array, findValue, equalsType, row, row, false);
            }
        };
    }

    private Type getEqualsType(ValueVector array, ValueVector findValue)
    {
        Type arrayType = array.type()
                .getType();
        if (arrayType == Type.Array)
        {
            arrayType = array.type()
                    .getSubType()
                    .getType();
        }
        Type findValueType = findValue.type()
                .getType();

        return arrayType.getPrecedence() > findValueType.getPrecedence() ? arrayType
                : findValueType;
    }
}
