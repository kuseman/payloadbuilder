package com.viskan.payloadbuilder.parser.tree;

import static java.util.Objects.requireNonNull;

/** Standard join producing tuples */
public class Join extends JoinPart
{
    private final Expression condition;
    private final JoinType type;

    public Join(JoinedTableSource joinedTableSource, JoinType type, Expression condition)
    {
        super(joinedTableSource);
        this.type = requireNonNull(type, "type");
        this.condition = requireNonNull(condition, "condition");
    }

    public enum JoinType
    {
        INNER,
        LEFT;
    }
    
    @Override
    public String toString()
    {
        return type + " JOIN " + joinedTableSource + "\tON " +  condition;
    }
}
