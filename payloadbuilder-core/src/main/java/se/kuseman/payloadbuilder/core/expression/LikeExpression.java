package se.kuseman.payloadbuilder.core.expression;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.IBooleanVectorBuilder;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;
import se.kuseman.payloadbuilder.api.expression.ILikeExpression;

/** Like expression */
public class LikeExpression implements ILikeExpression, Invertable
{
    private final IExpression expression;
    private final IExpression patternExpression;
    private final IExpression escapeCharacterExpression;
    private final boolean not;
    // private final Pattern pattern;

    public LikeExpression(IExpression expression, IExpression patternExpression, boolean not, IExpression escapeCharacterExpression)
    {
        this.expression = requireNonNull(expression, "expression");
        this.patternExpression = requireNonNull(patternExpression, "patternExpression");
        this.not = not;
        this.escapeCharacterExpression = escapeCharacterExpression;
        if (escapeCharacterExpression != null)
        {
            throw new IllegalArgumentException("Escape character is not implemented");
        }
    }

    @Override
    public IExpression getExpression()
    {
        return expression;
    }

    @Override
    public IExpression getPatternExpression()
    {
        return patternExpression;
    }

    @Override
    public IExpression getInvertedExpression()
    {
        return new LikeExpression(expression, patternExpression, !not, escapeCharacterExpression);
    }

    @Override
    public boolean isNot()
    {
        return not;
    }

    @Override
    public IExpression getEscapeCharacterExpression()
    {
        return escapeCharacterExpression;
    }

    @Override
    public ResolvedType getType()
    {
        return ResolvedType.of(Type.Boolean);
    }

    @Override
    public List<IExpression> getChildren()
    {
        return asList(expression, patternExpression);
    }

    @Override
    public <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public ValueVector eval(TupleVector input, IExecutionContext context)
    {
        Pattern pattern = null;
        ValueVector value = expression.eval(input, context);
        ValueVector patternVv = patternExpression.eval(input, context);
        // Constant regexp, create pattern once
        if (patternExpression.isConstant())
        {
            if (patternVv.isNull(0))
            {
                return ValueVector.literalNull(ResolvedType.of(Type.Boolean), input.getRowCount());
            }

            pattern = create(patternVv.getString(0)
                    .toString());
        }

        int rowCount = input.getRowCount();

        IBooleanVectorBuilder builder = context.getVectorBuilderFactory()
                .getBooleanVectorBuilder(rowCount);

        for (int i = 0; i < rowCount; i++)
        {
            Pattern currentPattern = pattern;
            if (currentPattern == null)
            {
                currentPattern = create(patternVv.getString(i)
                        .toString());
            }

            if (value.isNull(i))
            {
                builder.putNull();
                continue;
            }

            String matchValue = String.valueOf(value.getString(i));

            boolean result = currentPattern.matcher(matchValue)
                    .find();

            builder.put(not ? !result
                    : result);
        }
        return builder.build();
    }

    private Pattern create(String pattern)
    {
        // % -> .*?
        // _ -> .
        // [] -> not implemented yet
        // escape_char -> not implemented

        String regexPattern = "^" + pattern
                // Replace regex reserved chars
                .replace("?", "\\?")

                // Replace LIKE wildcards to regex
                .replace("%", ".*?")
                .replace("_", ".?") + "$";
        return Pattern.compile(regexPattern);
    }

    @Override
    public int hashCode()
    {
        // CSOFF
        int hashCode = 17;
        hashCode = hashCode * 37 + expression.hashCode();
        hashCode = hashCode * 37 + patternExpression.hashCode();
        return hashCode;
        // CSON
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
        else if (obj instanceof LikeExpression)
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
        return expression.toString() + (not ? " NOT"
                : "")
               + " LIKE "
               + patternExpression.toString();
    }

    @Override
    public String toVerboseString()
    {
        return expression.toVerboseString() + (not ? " NOT"
                : "")
               + " LIKE "
               + patternExpression.toVerboseString();
    }
}
