package se.kuseman.payloadbuilder.api.expression;

/** Definition of LIKE expression */
public interface ILikeExpression extends IExpression
{
    /** Return true if this is expression in a NOT IN */
    boolean isNot();

    /** Return pattern expression for this LIKE expression */
    IExpression getPatternExpression();
}
