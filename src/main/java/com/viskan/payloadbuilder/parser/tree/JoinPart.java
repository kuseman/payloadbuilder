package com.viskan.payloadbuilder.parser.tree;

import static java.util.Objects.requireNonNull;

public abstract class JoinPart extends JoinItem
{
    protected final JoinedTableSource joinedTableSource;
    
    public JoinPart(JoinedTableSource joinedTableSource)
    {
        this.joinedTableSource = requireNonNull(joinedTableSource, "joinedTableSource");
    }
}
