package com.viskan.payloadbuilder.parser.tree;

/** Option declared with a WITH clause */
public abstract class WithOption extends ANode
{
    @Override
    public <TR, TC> TR accept(TreeVisitor<TR, TC> visitor, TC context)
    {
        throw new RuntimeException("Should not be visited");
    }
}
