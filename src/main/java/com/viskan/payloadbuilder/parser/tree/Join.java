package com.viskan.payloadbuilder.parser.tree;

import static java.util.Objects.requireNonNull;

/** Standard join producing tuples */
public class Join extends AJoin
{
    private final Expression condition;
    private final JoinType type;

    public Join(
            TableSource tableSource,
            JoinType type,
            Expression condition)
    {
        super(tableSource);
        this.type = requireNonNull(type, "type");
        this.condition = requireNonNull(condition, "condition");
    }
    
    public JoinType getType()
    {
        return type;
    }
    
    public Expression getCondition()
    {
        return condition;
    }

    @Override
    public <TR, TC> TR accept(TreeVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);                
    }
    
    @Override
    public String toString()
    {
        return type + " JOIN " + getTableSource() + "\tON " +  condition;
    }

    public enum JoinType
    {
        INNER,
        LEFT;
    }
}
