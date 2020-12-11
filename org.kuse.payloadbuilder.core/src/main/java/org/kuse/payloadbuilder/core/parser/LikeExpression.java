package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

import java.util.Objects;
import java.util.regex.Pattern;

/** Like expression */
public class LikeExpression extends Expression
{
    private final Expression expression;
    private final Expression patternExpression;
    private final Expression escapeCharacterExpression;
    private final boolean not;
    private final Pattern pattern;

    public LikeExpression(Expression expression, Expression patternExpression, boolean not, Expression escapeCharacterExpression)
    {
        this.expression = requireNonNull(expression, "expression");
        this.patternExpression = requireNonNull(patternExpression, "patternExpression");
        this.not = not;
        this.escapeCharacterExpression = escapeCharacterExpression;
        this.pattern = null;
        if (escapeCharacterExpression != null)
        {
            throw new IllegalArgumentException("Escape character is not implemented");
        }
    }

    private LikeExpression(Expression expression, Expression patternExpression, Pattern pattern, boolean not, Expression escapeCharacterExpression)
    {
        this.expression = requireNonNull(expression, "expression");
        this.patternExpression = requireNonNull(patternExpression, "patternExpression");
        this.pattern = requireNonNull(pattern, "pattern");
        this.not = not;
        this.escapeCharacterExpression = escapeCharacterExpression;
    }

    public Expression getExpression()
    {
        return expression;
    }

    public Expression getPatternExpression()
    {
        return patternExpression;
    }

    public boolean isNot()
    {
        return not;
    }

    public Expression getEscapeCharacterExpression()
    {
        return escapeCharacterExpression;
    }

    @Override
    public boolean isConstant()
    {
        return expression.isConstant()
            && patternExpression.isConstant()
            && (escapeCharacterExpression == null || escapeCharacterExpression.isConstant());
    }

    @Override
    public boolean isNullable()
    {
        return false;
    }

    @Override
    public Expression fold()
    {
        // Create pattern if constant to avoid re-creating on each evaluation
        if (patternExpression instanceof LiteralStringExpression)
        {
            Pattern pattern = create(((LiteralStringExpression) patternExpression).getValue());
            return new LikeExpression(expression, patternExpression, pattern, not, escapeCharacterExpression);
        }

        return this;
    }

    @Override
    public Object eval(ExecutionContext context)
    {
        Object value = expression.eval(context);
        if (value == null)
        {
            return null;
        }

        String matchValue = String.valueOf(value);
        Pattern pattern;
        if (this.pattern != null)
        {
            pattern = this.pattern;
        }
        else
        {
            value = patternExpression.eval(context);
            if (value == null)
            {
                return null;
            }

            String stringPattern = String.valueOf(value);
            pattern = create(stringPattern);
        }
        boolean matches = pattern.matcher(matchValue).find();
        return not ? !matches : matches;
    }

    private Pattern create(String pattern)
    {
        // %           -> .*?
        // _           -> .
        // []          -> not implemented yet
        // escape_char -> not implemented

        String regexPattern = "^"
            + pattern
                    // Replace regex reserved chars
                    .replace("?", "\\?")

                    // Replace LIKE wildcards to regex
                    .replace("%", ".*?")
                    .replace("_", ".?")
            + "$";
        return Pattern.compile(regexPattern);
    }

    @Override
    public <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public int hashCode()
    {
        //CSOFF
        int hashCode = 17;
        hashCode = hashCode * 37 + expression.hashCode();
        hashCode = hashCode * 37 + patternExpression.hashCode();
        return hashCode;
        //CSON
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof LikeExpression)
        {
            LikeExpression that = (LikeExpression) obj;
            return expression.equals(that.expression)
                && patternExpression.equals(that.patternExpression)
                && not == that.not
                && Objects.equals(escapeCharacterExpression, that.escapeCharacterExpression);
        }

        return false;
    }

    @Override
    public String toString()
    {
        return expression.toString()
            + (not ? " NOT" : "")
            + " LIKE "
            + patternExpression.toString();
    }
}
