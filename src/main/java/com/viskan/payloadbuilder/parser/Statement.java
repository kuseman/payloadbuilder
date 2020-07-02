package com.viskan.payloadbuilder.parser;

/** Base class for statements */
public abstract class Statement
{
    /** Accept method for visitor */
    public abstract <TR, TC> TR accept(StatementVisitor<TR, TC> visitor, TC context);
}
