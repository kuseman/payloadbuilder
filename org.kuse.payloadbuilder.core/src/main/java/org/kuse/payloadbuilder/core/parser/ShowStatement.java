package org.kuse.payloadbuilder.core.parser;

/** Show statement for querying current parameters/variables */
public class ShowStatement extends Statement
{
    private final Type type;
    
    ShowStatement(Type type)
    {
        this.type = type;
    }
    
    public Type getType()
    {
        return type;
    }
    
    @Override
    public <TR, TC> TR accept(StatementVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }
    
    public enum Type
    {
        PARAMETERS,
        VARIABLES
    }
}
