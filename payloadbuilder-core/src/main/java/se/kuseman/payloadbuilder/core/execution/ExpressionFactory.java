package se.kuseman.payloadbuilder.core.execution;

import se.kuseman.payloadbuilder.api.execution.ObjectVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpressionFactory;
import se.kuseman.payloadbuilder.api.expression.ILiteralArrayExpression;
import se.kuseman.payloadbuilder.api.expression.ILiteralBooleanExpression;
import se.kuseman.payloadbuilder.api.expression.ILiteralDoubleExpression;
import se.kuseman.payloadbuilder.api.expression.ILiteralFloatExpression;
import se.kuseman.payloadbuilder.api.expression.ILiteralIntegerExpression;
import se.kuseman.payloadbuilder.api.expression.ILiteralLongExpression;
import se.kuseman.payloadbuilder.api.expression.ILiteralObjectExpression;
import se.kuseman.payloadbuilder.api.expression.ILiteralStringExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralArrayExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralBooleanExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralDoubleExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralFloatExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralIntegerExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralLongExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralObjectExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralStringExpression;

/** Implementation of {@link IExpressionFactory} */
class ExpressionFactory implements IExpressionFactory
{
    @Override
    public ILiteralBooleanExpression createBooleanExpression(boolean value)
    {
        return value ? LiteralBooleanExpression.TRUE
                : LiteralBooleanExpression.FALSE;
    }

    @Override
    public ILiteralIntegerExpression createIntegerExpression(int value)
    {
        return new LiteralIntegerExpression(value);
    }

    @Override
    public ILiteralLongExpression createLongExpression(long value)
    {
        return new LiteralLongExpression(value);
    }

    @Override
    public ILiteralFloatExpression createFloatExpression(float value)
    {
        return new LiteralFloatExpression(value);
    }

    @Override
    public ILiteralDoubleExpression createDoubleExpression(double value)
    {
        return new LiteralDoubleExpression(value);
    }

    @Override
    public ILiteralStringExpression createStringExpression(UTF8String value)
    {
        return new LiteralStringExpression(value);
    }

    @Override
    public ILiteralArrayExpression createArrayExpression(ValueVector array)
    {
        return new LiteralArrayExpression(array);
    }

    @Override
    public ILiteralObjectExpression createObjectExpression(ObjectVector object)
    {
        return new LiteralObjectExpression(object);
    }
}
