package se.kuseman.payloadbuilder.core.parser;

/** Base class for statements */
// CSOFF
public abstract class Statement
// CSON
{
    /** Accept method for visitor */
    public abstract <TR, TC> TR accept(StatementVisitor<TR, TC> visitor, TC context);
}
