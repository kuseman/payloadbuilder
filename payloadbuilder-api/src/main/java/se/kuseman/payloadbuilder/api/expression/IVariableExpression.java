package se.kuseman.payloadbuilder.api.expression;

/** Variable expression @var */
public interface IVariableExpression extends IExpression
{
    /** Return name of variable */
    String getName();

    /** Return true if this is a system variable otherwise false */
    boolean isSystem();
}
