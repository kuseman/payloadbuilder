package se.kuseman.payloadbuilder.api.expression;

import java.util.List;

/** Definition of a an IN expression */
public interface IInExpression extends IExpression
{
    /** Return true if this is expression in a NOT IN */
    boolean isNot();

    /** Return IN arguments */
    List<? extends IExpression> getArguments();
}
