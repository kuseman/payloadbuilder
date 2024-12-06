package se.kuseman.payloadbuilder.api.expression;

/** Base class for expression visitors. */
public class AExpressionVisitor<T, C> implements IExpressionVisitor<T, C>
{
    protected T defaultResult(C context)
    {
        return null;
    }

    protected T aggregate(T result, T nextResult)
    {
        return nextResult;
    }

    @Override
    public T visitChildren(C context, IExpression expression)
    {
        T result = defaultResult(context);
        for (IExpression p : expression.getChildren())
        {
            T value = p.accept(this, context);
            result = aggregate(result, value);
        }
        return result;
    }
}
