package se.kuseman.payloadbuilder.core.statement;

import se.kuseman.payloadbuilder.api.expression.IExpression;

/**
 * Visitor definition of statements
 *
 * @param <TR> Return type
 * @param <TC> Context type
 */
public interface StatementVisitor<TR, TC>
{
    /** Visit children of provided statement */
    default TR visitChildren(Statement statement, TC context)
    {
        TR result = defaultResult(context);
        for (Statement child : statement.getChildren())
        {
            TR value = child.accept(this, context);
            result = aggregate(result, value);
        }
        return result;
    }

    default TR defaultResult(TC context)
    {
        return null;
    }

    default TR aggregate(TR result, TR nextResult)
    {
        return nextResult;
    }

    /** Expression visit. Implementations can override this method to catch all expressions during a {@link Statement} traversal. */
    default void visit(IExpression expression, TC context)
    {
    }

    // CSOFF
    /* Control flow nodes */
    default TR visit(IfStatement statement, TC context)
    {
        visit(statement.getCondition(), context);
        return visitChildren(statement, context);
    }

    default TR visit(PrintStatement statement, TC context)
    {
        visit(statement.getExpression(), context);
        return visitChildren(statement, context);
    }

    /* Misc nodes */
    default TR visit(SetStatement statement, TC context)
    {
        visit(statement.getExpression(), context);
        return visitChildren(statement, context);
    }

    default TR visit(UseStatement statement, TC context)
    {
        visit(statement.getExpression(), context);
        return visitChildren(statement, context);
    }

    default TR visit(DescribeSelectStatement statement, TC context)
    {
        return visitChildren(statement, context);
    }

    default TR visit(ShowStatement statement, TC context)
    {
        return visitChildren(statement, context);
    }

    default TR visit(CacheFlushRemoveStatement statement, TC context)
    {
        visit(statement.getKey(), context);
        return visitChildren(statement, context);
    }

    default TR visit(StatementList statement, TC context)
    {
        return visitChildren(statement, context);
    }

    /* DQL nodes */
    default TR visit(LogicalSelectStatement statement, TC context)
    {
        return visitChildren(statement, context);
    }

    default TR visit(PhysicalSelectStatement statement, TC context)
    {
        return visitChildren(statement, context);
    }

    /* DML nodes */
    default TR visit(InsertIntoStatement statement, TC context)
    {
        return visitChildren(statement, context);
    }

    /* DDL nodes */
    default TR visit(DropTableStatement statement, TC context)
    {
        return visitChildren(statement, context);
    }
    // CSON
}
