package se.kuseman.payloadbuilder.api.expression;

import se.kuseman.payloadbuilder.api.execution.ObjectVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Definition of an expression factory used when creating new expressions when folding etc. */
public interface IExpressionFactory
{
    /** Creates a literal boolean expression */
    ILiteralBooleanExpression createBooleanExpression(boolean value);

    /** Creates a literal integer expression */
    ILiteralIntegerExpression createIntegerExpression(int value);

    /** Creates a literal long expression */
    ILiteralLongExpression createLongExpression(long value);

    /** Creates a literal float expression */
    ILiteralFloatExpression createFloatExpression(float value);

    /** Creates a literal double expression */
    ILiteralDoubleExpression createDoubleExpression(double value);

    /** Creates a literal string expression */
    ILiteralStringExpression createStringExpression(UTF8String value);

    /** Creates a literal array expression */
    ILiteralArrayExpression createArrayExpression(ValueVector array);

    /** Creates a literal object expression */
    ILiteralObjectExpression createObjectExpression(ObjectVector object);
}
