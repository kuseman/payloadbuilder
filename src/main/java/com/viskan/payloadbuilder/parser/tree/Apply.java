package com.viskan.payloadbuilder.parser.tree;

import static java.util.Objects.requireNonNull;

public class Apply extends AJoin
{
    private final JoinedTableSource joinedTableSource;
    private final ApplyType type;
    
    public Apply(JoinedTableSource joinedTableSource, ApplyType type)
    {
        this.joinedTableSource = requireNonNull(joinedTableSource, "joinedTableSource");
        this.type = requireNonNull(type, "type");
    }
    
    public JoinedTableSource getJoinedTableSource()
    {
        return joinedTableSource;
    }
    
    public ApplyType getType()
    {
        return type;
    }
    
    @Override
    public <TR, TC> TR accept(TreeVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);                
    }
    
    public enum ApplyType
    {
        OUTER,
        CROSS;
    }
}
