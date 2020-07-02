package com.viskan.payloadbuilder.parser;

/** Option declared with a WITH clause */
public abstract class WithOption extends ASelectNode
{
    @Override
    public <TR, TC> TR accept(SelectVisitor<TR, TC> visitor, TC context)
    {
        throw new RuntimeException("Should not be visited");
    }
}
