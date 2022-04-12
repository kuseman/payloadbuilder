package se.kuseman.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

import org.antlr.v4.runtime.Token;

import se.kuseman.payloadbuilder.api.TableAlias;

/**
 * An unresolved sub query expression.
 *
 * <pre>
 * Used when a scalar value should be evaluated from a query
 * ie.
 *
 * select col1
 * ,      (select 1,2 for object) myCol
 * </pre>
 */
public class UnresolvedSubQueryExpression extends Expression
{
    private final SelectStatement selectStatement;
    private final TableAlias tableAlias;
    private final Token token;

    public UnresolvedSubQueryExpression(SelectStatement selectStatement, TableAlias tableAlias, Token token)
    {
        this.selectStatement = requireNonNull(selectStatement);
        this.tableAlias = tableAlias;
        this.token = token;
    }

    public SelectStatement getSelectStatement()
    {
        return selectStatement;
    }

    public TableAlias getTableAlias()
    {
        return tableAlias;
    }

    public Token getToken()
    {
        return token;
    }

    @Override
    public <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }
}
