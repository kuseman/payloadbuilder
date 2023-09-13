package se.kuseman.payloadbuilder.api.expression;

import java.util.List;

/** A lambda expression */
public interface ILambdaExpression extends IUnaryExpression
{
    /** Get lambda identifiers */
    List<String> getIdentifiers();
}
