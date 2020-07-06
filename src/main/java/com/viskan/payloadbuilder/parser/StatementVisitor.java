package com.viskan.payloadbuilder.parser;

/** Visitor definition of statements */
public interface StatementVisitor<TR, TC>
{
    /* Control flow nodes */
    TR visit(IfStatement statement, TC context);
    TR visit(PrintStatement statement, TC context);
    
    /* Misc nodes */
    TR visit(SetStatement statement, TC context);
    TR visit(DescribeTableStatement statement, TC context);
    TR visit(DescribeFunctionStatement statement, TC context);
    TR visit(DescribeSelectStatement statement, TC context);
    
    /* DML nodes */
    TR visit(SelectStatement statement, TC context);
}
