package com.viskan.payloadbuilder.parser.tree;

import static java.util.Objects.requireNonNull;

public class Apply extends JoinPart
{
    private final ApplyType type;
    
    public Apply(JoinedTableSource joinedTableSource, ApplyType type)
    {
        super(joinedTableSource);
        this.type = requireNonNull(type, "type");
    }
    
    public ApplyType getType()
    {
        return type;
    }
    
    public enum ApplyType
    {
        OUTER,
        CROSS;
    }
}
