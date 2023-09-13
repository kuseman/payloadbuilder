package se.kuseman.payloadbuilder.api.expression;

/** IS (NOT) NULL expression */
public interface INullPredicateExpression extends IUnaryExpression
{
    /** Return true if this expression is a NOT type of null predicate */
    boolean isNot();
}
