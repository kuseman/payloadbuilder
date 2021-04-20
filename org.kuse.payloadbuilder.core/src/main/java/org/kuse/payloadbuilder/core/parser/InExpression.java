package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.kuse.payloadbuilder.core.parser.LiteralBooleanExpression.FALSE_LITERAL;
import static org.kuse.payloadbuilder.core.parser.LiteralBooleanExpression.TRUE_LITERAL;
import static org.kuse.payloadbuilder.core.parser.LiteralNullExpression.NULL_LITERAL;

import java.util.List;

import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.utils.ExpressionMath;

/** In expression */
public class InExpression extends Expression
{
    private final Expression expression;
    private final List<Expression> arguments;
    private final boolean not;

    public InExpression(Expression expression, List<Expression> arguments, boolean not)
    {
        this.not = not;
        this.expression = requireNonNull(expression, "expression");
        this.arguments = requireNonNull(arguments, "arguments");
    }

    public boolean isNot()
    {
        return not;
    }

    public Expression getExpression()
    {
        return expression;
    }

    public List<Expression> getArguments()
    {
        return arguments;
    }

    @Override
    public boolean isConstant()
    {
        return expression.isConstant()
            && arguments.stream().allMatch(Expression::isConstant);
    }

    @Override
    public Expression fold()
    {
        boolean doFold = expression instanceof LiteralExpression;

        for (Expression argument : arguments)
        {
            doFold = doFold && argument instanceof LiteralExpression;
        }

        if (doFold)
        {
            Object value = ((LiteralExpression) expression).getObjectValue();
            if (value == null)
            {
                return NULL_LITERAL;
            }
            for (Expression arg : arguments)
            {
                Object argValue = ((LiteralExpression) arg).getObjectValue();
                if (argValue == null)
                {
                    continue;
                }
                if (ExpressionMath.inValue(value, argValue))
                {
                    return not ? FALSE_LITERAL : TRUE_LITERAL;
                }
            }

            return not ? TRUE_LITERAL : FALSE_LITERAL;
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

        int size = arguments.size();
        for (int i = 0; i < size; i++)
        {
            Object argValue = arguments.get(i).eval(context);
            if (argValue == null)
            {
                continue;
            }
            if (ExpressionMath.inValue(value, argValue))
            {
                return not ? false : true;
            }
        }
        return not ? true : false;
    }

    //
    //    @Override
    //    public ExpressionCode generateCode(CodeGeneratorContext context, ExpressionCode parentCode)
    //    {
    //        ExpressionCode childCode = expression.generateCode(context, parentCode);
    //        ExpressionCode code = ExpressionCode.code(context);
    //
    //        int size = arguments.size();
    //        // Generate code for
    //        StringBuilder sb = new StringBuilder();
    //        sb.append(childCode.getCode());
    //
    //        sb.append("boolean ").append(code.getResVar()).append(" = false;\n");
    //        sb.append("boolean ").append(code.getIsNull()).append(" = ").append(childCode.getIsNull()).append(";\n");
    //
    //        String template = "if (!%s && !%s)\n"
    //            + "{\n"
    //            + "  %s\n"
    //            + "  if (!%s)\n"
    //            + "  {\n"
    //            + "    %s = ExpressionMath.inValue(%s, %s);\n"
    //            + "    %s = false;\n"
    //            + "  }\n"
    //            + "}\n";
    //
    //        for (int i = 0; i < size; i++)
    //        {
    //            Expression arg = arguments.get(i);
    //            ExpressionCode argCode = arg.generateCode(context, parentCode);
    //            sb.append(String.format(template,
    //                    code.getIsNull(), code.getResVar(),
    //                    argCode.getCode(),
    //                    argCode.getIsNull(),
    //                    code.getResVar(), childCode.getResVar(), argCode.getResVar(),
    //                    code.getIsNull()));
    //        }
    //
    //        code.setCode(sb.toString());
    //        return code;
    //    }

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
        hashCode = hashCode * 37 + arguments.hashCode();
        return hashCode;
        //CSON
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof InExpression)
        {
            InExpression that = (InExpression) obj;
            return expression.equals(that.expression)
                && arguments.equals(that.arguments);
        }

        return false;
    }

    @Override
    public String toString()
    {
        return expression.toString() + (not ? " NOT" : "") + " IN (" + arguments.stream().map(e -> e.toString()).collect(joining(", ")) + ")";
    }
}
