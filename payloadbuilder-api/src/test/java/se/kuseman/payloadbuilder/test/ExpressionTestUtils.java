package se.kuseman.payloadbuilder.test;

import static java.util.Objects.requireNonNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import org.mockito.Mockito;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.ILiteralStringExpression;

/** Test utils when working with {@link IExpression}'s */
public class ExpressionTestUtils
{
    /** Create a {@ILiteralStringExpression} with provided value */
    public static ILiteralStringExpression createStringExpression(String value)
    {
        requireNonNull(value);
        ILiteralStringExpression mock = Mockito.mock(ILiteralStringExpression.class);
        when(mock.eval(any(IExecutionContext.class))).thenReturn(VectorTestUtils.vv(Type.String, value));
        when(mock.getValue()).thenReturn(UTF8String.from(value));
        return mock;
    }
}
