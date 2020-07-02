package com.viskan.payloadbuilder.parser;

/** Base class for tree nodes */
public abstract class ASelectNode
{
    /*
     * Statement
     * Expression
     * Select
     * 
     * ANode
     *  Statement
     *    If
     *    Print
     *    Select
     *      
     * 
     */
    
    public abstract <TR, TC> TR accept(SelectVisitor<TR, TC> visitor, TC context);
}
