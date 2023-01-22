package se.kuseman.payloadbuilder.core.statement;

/**
 * Visitor definition of statements
 *
 * @param <TR> Return type
 * @param <TC> Context type
 */
public interface StatementVisitor<TR, TC>
{
    // CSOFF
    /* Control flow nodes */
    TR visit(IfStatement statement, TC context);

    TR visit(PrintStatement statement, TC context);

    /* Misc nodes */
    TR visit(SetStatement statement, TC context);

    TR visit(UseStatement statement, TC context);

    TR visit(DescribeSelectStatement statement, TC context);

    TR visit(ShowStatement statement, TC context);

    TR visit(CacheFlushRemoveStatement statement, TC context);

    TR visit(StatementList statement, TC context);

    /* DQL nodes */
    TR visit(LogicalSelectStatement statement, TC context);

    TR visit(PhysicalSelectStatement statement, TC context);

    /* DML */
    TR visit(InsertIntoStatement statement, TC context);

    /* DDL nodes */
    TR visit(DropTableStatement statement, TC context);
    // CSON

}
