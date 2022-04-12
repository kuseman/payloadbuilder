package se.kuseman.payloadbuilder.core.parser;

/** Base class for tree nodes */
public abstract class ASelectNode
{
    /** Visitor accept definition */
    public abstract <TR, TC> TR accept(SelectVisitor<TR, TC> visitor, TC context);
}
