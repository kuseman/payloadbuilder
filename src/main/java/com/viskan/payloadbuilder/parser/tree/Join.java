package com.viskan.payloadbuilder.parser.tree;

import static java.util.Objects.requireNonNull;

/** Standard join producing tuples */
public class Join extends AJoin
{
    private final JoinedTableSource joinedTableSource;
    private final Expression condition;
    private final JoinType type;

    public Join(JoinedTableSource joinedTableSource, JoinType type, Expression condition)
    {
        this.joinedTableSource = requireNonNull(joinedTableSource, "joinedTableSource");
        this.type = requireNonNull(type, "type");
        this.condition = requireNonNull(condition, "condition");
    }
    
    public JoinedTableSource getJoinedTableSource()
    {
        return joinedTableSource;
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
        return type + " JOIN " + joinedTableSource + "\tON " +  condition;
    }

    public enum JoinType
    {
        INNER,
        LEFT;
    }
}
