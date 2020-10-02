package org.kuse.payloadbuilder.core.parser;

/** Base class for tree nodes */
public abstract class ASelectNode
{
    public abstract <TR, TC> TR accept(SelectVisitor<TR, TC> visitor, TC context);
}
