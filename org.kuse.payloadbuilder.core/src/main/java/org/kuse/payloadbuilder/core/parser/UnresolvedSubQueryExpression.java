package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

import org.kuse.payloadbuilder.core.parser.Select.For;

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

    public UnresolvedSubQueryExpression(SelectStatement selectStatement)
    {
        this.selectStatement = requireNonNull(selectStatement);

        For forOutput = selectStatement.getSelect().getForOutput();
        for (SelectItem item : selectStatement.getSelect().getSelectItems())
        {
            if (forOutput == For.ARRAY && !item.isEmptyIdentifier())
            {
                throw new ParseException("All select items in ARRAY output must have empty identifiers", item.getToken());
            }
            else if ((forOutput == For.OBJECT || forOutput == For.OBJECT_ARRAY) && isBlank(item.getIdentifier()))
            {
                throw new ParseException("All select items in OBJECT output must have identifiers", item.getToken());
            }
        }
    }

    public SelectStatement getSelectStatement()
    {
        return selectStatement;
    }

    @Override
    public <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }
}
