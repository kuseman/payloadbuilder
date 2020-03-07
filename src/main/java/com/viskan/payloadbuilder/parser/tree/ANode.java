package com.viskan.payloadbuilder.parser.tree;

/** Base class for tree nodes */
public abstract class ANode
{
    public abstract <TR, TC> TR accept(TreeVisitor<TR, TC> visitor, TC context);
}
