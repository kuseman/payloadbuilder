package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Base64.Encoder;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.MutableValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Scalar function that encodes/decodes base64 strings. */
class Base64Function extends ScalarFunctionInfo
{
    private static final Decoder DECODER = Base64.getDecoder();
    private static final Encoder ENCODER = Base64.getEncoder();

    private final boolean decode;

    Base64Function(boolean decode)
    {
        super("base64_" + (decode ? "decode"
                : "encode"), FunctionType.SCALAR);
        this.decode = decode;
    }

    @Override
    public Arity arity()
    {
        return Arity.ONE;
    }

    @Override
    public ResolvedType getType(List<IExpression> arguments)
    {
        return decode ? ResolvedType.ANY
                : ResolvedType.STRING;
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        return evalScalar(context, input, ValueVector.range(0, input.getRowCount()), catalogAlias, arguments);
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, ValueVector selection, String catalogAlias, List<IExpression> arguments)
    {
        ValueVector value = arguments.get(0)
                .eval(input, selection, context);

        int size = selection.size();
        MutableValueVector result = context.getVectorFactory()
                .getMutableVector(getType(arguments), size);

        for (int i = 0; i < size; i++)
        {
            if (value.isNull(i))
            {
                result.setNull(i);
            }
            else
            {
                String str = value.getString(i)
                        .toString();

                if (decode)
                {
                    byte[] bytes = DECODER.decode(str);
                    result.setAny(i, bytes);
                }
                else
                {
                    String val = ENCODER.encodeToString(str.getBytes());
                    result.setString(i, UTF8String.from(val));
                }
            }
        }
        return result;
    }
}
