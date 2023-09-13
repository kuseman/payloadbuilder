package se.kuseman.payloadbuilder.api.expression;

import static java.util.Collections.singletonList;

import java.util.List;

/** Base interface for unary expressions */
public interface IUnaryExpression extends IExpression
{
    /** Return target expression */
    IExpression getExpression();

    @Override
    default List<IExpression> getChildren()
    {
        return singletonList(getExpression());
    }
}
