package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.BooleanUtils;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;

/** CASE WHEN expression */
class CaseExpression extends Expression
{
    private final List<WhenClause> whenClauses;
    private final Expression elseExpression;

    CaseExpression(List<WhenClause> whenClauses, Expression elseExpression)
    {
        this.whenClauses = requireNonNull(whenClauses, "whenClauses");
        this.elseExpression = elseExpression;
    }

    List<WhenClause> getWhenClauses()
    {
        return whenClauses;
    }

    Expression getElseExpression()
    {
        return elseExpression;
    }

    @Override
    public boolean isConstant()
    {
        return whenClauses.stream().allMatch(w -> w.isConstant())
            && (elseExpression == null || elseExpression.isConstant());
    }

    @Override
    public <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public Object eval(ExecutionContext context)
    {
        int size = whenClauses.size();
        for (int i = 0; i < size; i++)
        {
            WhenClause whenClause = whenClauses.get(i);
            Boolean result = (Boolean) whenClause.condition.eval(context);
            if (BooleanUtils.isTrue(result))
            {
                return whenClause.result.eval(context);
            }
        }

        if (elseExpression != null)
        {
            return elseExpression.eval(context);
        }

        return null;
    }

    /** When clause */
    static class WhenClause
    {
        private final Expression condition;
        private final Expression result;

        WhenClause(Expression condition, Expression result)
        {
            this.condition = requireNonNull(condition, "condition");
            this.result = requireNonNull(result, "result");
        }

        Expression getCondition()
        {
            return condition;
        }

        Expression getResult()
        {
            return result;
        }

        boolean isConstant()
        {
            return condition.isConstant()
                && result.isConstant();
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(condition, result);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj instanceof WhenClause)
            {
                WhenClause that = (WhenClause) obj;
                return condition.equals(that.condition)
                    && result.equals(that.result);
            }
            return false;
        }
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(whenClauses, elseExpression);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof CaseExpression)
        {
            CaseExpression that = (CaseExpression) obj;
            return whenClauses.equals(that.whenClauses)
                && Objects.equals(elseExpression, that.elseExpression);
        }
        return false;
    }

    @Override
    public String toString()
    {
        /*
         * CASE
         *      WHEN .....
         *      WHEN .....
         *      ELSE ....
         * END
         *
         */

        return "CASE " + System.lineSeparator()
            + whenClauses.stream()
                    .map(w -> "\tWHEN " + w.condition + " THEN " + w.result)
                    .collect(joining(System.lineSeparator()))
            + System.lineSeparator()
            + (elseExpression != null ? ("\tELSE " + elseExpression) : "") + System.lineSeparator()
            + "END";
    }
}
