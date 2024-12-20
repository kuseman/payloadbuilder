package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;

import org.apache.commons.lang3.StringUtils;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.MutableValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Table valued function that splits a string and returns a table from the splitted result */
class StringSplitScalarFunction extends ScalarFunctionInfo
{
    StringSplitScalarFunction()
    {
        super("string_split", FunctionType.SCALAR);
    }

    @Override
    public ResolvedType getType(List<IExpression> arguments)
    {
        return ResolvedType.array(ResolvedType.STRING);
    }

    @Override
    public Arity arity()
    {
        return Arity.TWO;
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        return evalScalar(context, input, ValueVector.range(0, input.getRowCount()), catalogAlias, arguments);
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, ValueVector selection, String catalogAlias, List<IExpression> arguments)
    {
        final ValueVector value = arguments.get(0)
                .eval(context);
        final ValueVector separator = arguments.get(1)
                .eval(context);
        int rowCount = selection.size();
        MutableValueVector resultVector = context.getVectorFactory()
                .getMutableVector(ResolvedType.array(ResolvedType.STRING), rowCount);

        for (int i = 0; i < rowCount; i++)
        {
            if (value.isNull(i)
                    || separator.isNull(i))
            {
                resultVector.setNull(i);
                continue;
            }

            String strValue = value.getString(i)
                    .toString();
            String sepString = separator.getString(i)
                    .toString();
            String[] parts = StringUtils.split(strValue, sepString);

            int length = parts.length;
            MutableValueVector arrayVector = context.getVectorFactory()
                    .getMutableVector(ResolvedType.of(Type.String), length);
            for (int j = 0; j < length; j++)
            {
                arrayVector.setString(j, UTF8String.from(parts[j]));
            }

            resultVector.setArray(i, arrayVector);
        }
        return resultVector;
    }
}
