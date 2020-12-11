package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;
import static org.kuse.payloadbuilder.core.parser.LiteralBooleanExpression.FALSE_LITERAL;
import static org.kuse.payloadbuilder.core.parser.LiteralBooleanExpression.TRUE_LITERAL;
import static org.kuse.payloadbuilder.core.parser.LiteralNullExpression.NULL_LITERAL;

import org.kuse.payloadbuilder.core.codegen.CodeGeneratorContext;
import org.kuse.payloadbuilder.core.codegen.ExpressionCode;
import org.kuse.payloadbuilder.core.parser.ComparisonExpression.Type;

/** Logical not */
public class LogicalNotExpression extends Expression
{
    private final Expression expression;

    public LogicalNotExpression(Expression expression)
    {
        this.expression = requireNonNull(expression, "expression");
    }

    public Expression getExpression()
    {
        return expression;
    }

    @Override
    public boolean isConstant()
    {
        return expression.isConstant();
    }

    @Override
    public Expression fold()
    {
        if (expression instanceof LiteralNullExpression)
        {
            return NULL_LITERAL;
        }
        else if (expression instanceof LiteralBooleanExpression)
        {
            boolean value = ((LiteralBooleanExpression) expression).getValue();
            return value ? FALSE_LITERAL : TRUE_LITERAL;
        }
        else if (expression instanceof ComparisonExpression)
        {
            ComparisonExpression ce = (ComparisonExpression) expression;
            switch (ce.getType())
            {
                case EQUAL:
                    // =  >=  !=
                    return new ComparisonExpression(Type.NOT_EQUAL, ce.getLeft(), ce.getRight());
                case GREATER_THAN:
                    // >  >= <=
                    return new ComparisonExpression(Type.LESS_THAN_EQUAL, ce.getLeft(), ce.getRight());
                case GREATER_THAN_EQUAL:
                    // >=  >= <
                    return new ComparisonExpression(Type.LESS_THAN, ce.getLeft(), ce.getRight());
                case LESS_THAN:
                    // <  >= >=
                    return new ComparisonExpression(Type.GREATER_THAN_EQUAL, ce.getLeft(), ce.getRight());
                case LESS_THAN_EQUAL:
                    // <=  >= >
                    return new ComparisonExpression(Type.GREATER_THAN, ce.getLeft(), ce.getRight());
                case NOT_EQUAL:
                    // !=  >=  =
                    return new ComparisonExpression(Type.EQUAL, ce.getLeft(), ce.getRight());
                default:
                    throw new IllegalArgumentException("Unkown comparison type " + ce.getType());
            }
        }
        else if (expression instanceof LogicalBinaryExpression)
        {
            LogicalBinaryExpression lbe = (LogicalBinaryExpression) expression;
            if (lbe.getType() == LogicalBinaryExpression.Type.AND)
            {
                return new LogicalBinaryExpression(
                        LogicalBinaryExpression.Type.OR,
                        new LogicalNotExpression(lbe.getLeft()),
                        new LogicalNotExpression(lbe.getRight()));
            }

            return new LogicalBinaryExpression(
                    LogicalBinaryExpression.Type.AND,
                    new LogicalNotExpression(lbe.getLeft()),
                    new LogicalNotExpression(lbe.getRight()));
        }
        else if (expression instanceof LogicalNotExpression)
        {
            return ((LogicalNotExpression) expression).getExpression();
        }
        else if (expression instanceof LikeExpression)
        {
            LikeExpression le = (LikeExpression) expression;
            return new LikeExpression(le.getExpression(), le.getPatternExpression(), !le.isNot(), le.getEscapeCharacterExpression());
        }
        else if (expression instanceof InExpression)
        {
            InExpression ie = (InExpression) expression;
            return new InExpression(ie.getExpression(), ie.getArguments(), !ie.isNot());
        }
        return this;
    }

    @Override
    public boolean isNullable()
    {
        return expression.isNullable();
    }

    @Override
    public Class<?> getDataType()
    {
        return expression.getDataType();
    }

    @Override
    public Object eval(ExecutionContext context)
    {
        Object result = expression.eval(context);
        return result != null ? !(Boolean) result : null;
    }

    @Override
    public ExpressionCode generateCode(CodeGeneratorContext context, ExpressionCode parentCode)
    {
        ExpressionCode childCode = expression.generateCode(context, parentCode);

        if (expression.isNullable())
        {
            boolean addCast = !Boolean.class.isAssignableFrom(expression.getDataType());

            String template = "%s\n"
                + "if (!%s)\n"
                + "{\n"
                + "  %s = !%s%s;\n"
                + "}\n";

            childCode.setCode(String.format(template,
                    childCode.getCode(),
                    childCode.getIsNull(),
                    childCode.getResVar(),
                    addCast ? "(Boolean)" : "", childCode.getResVar()));
        }
        else
        {
            String template = "%s\n"
                + "%s = !%s;\n";
            childCode.setCode(String.format(template,
                    childCode.getCode(),
                    childCode.getResVar(), childCode.getResVar()));
        }

        childCode.setCode("// NOT\n" + childCode.getCode());
        return childCode;
    }

    @Override
    public <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public int hashCode()
    {
        return expression.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof LogicalNotExpression)
        {
            return expression.equals(((LogicalNotExpression) obj).expression);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "NOT " + expression.toString();
    }
}
