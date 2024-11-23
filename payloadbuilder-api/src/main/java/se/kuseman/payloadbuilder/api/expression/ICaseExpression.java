package se.kuseman.payloadbuilder.api.expression;

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Objects;

/** Case when expression */
public interface ICaseExpression extends IExpression
{
    /** Return when clauses */
    List<WhenClause> getWhenClauses();

    /** Return else expression if any */
    IExpression getElseExpression();

    @Override
    default <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }

    /** When clause */
    public static class WhenClause
    {
        private final IExpression condition;
        private final IExpression result;

        public WhenClause(IExpression condition, IExpression result)
        {
            this.condition = requireNonNull(condition, "condition");
            this.result = requireNonNull(result, "result");
        }

        public IExpression getCondition()
        {
            return condition;
        }

        public IExpression getResult()
        {
            return result;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(condition, result);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj == null)
            {
                return false;
            }
            else if (obj == this)
            {
                return true;
            }
            else if (obj instanceof WhenClause)
            {
                WhenClause that = (WhenClause) obj;
                return condition.equals(that.condition)
                        && result.equals(that.result);
            }
            return false;
        }
    }
}
