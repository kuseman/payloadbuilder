package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;
import static org.kuse.payloadbuilder.core.parser.LiteralBooleanExpression.FALSE_LITERAL;
import static org.kuse.payloadbuilder.core.parser.LiteralBooleanExpression.TRUE_LITERAL;

import org.kuse.payloadbuilder.core.codegen.CodeGeneratorContext;
import org.kuse.payloadbuilder.core.codegen.ExpressionCode;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;

/** Logical binary expression */
public class LogicalBinaryExpression extends Expression
{
    private final Type type;
    private final Expression left;
    private final Expression right;

    public LogicalBinaryExpression(Type type, Expression left, Expression right)
    {
        this.type = requireNonNull(type, "type");
        this.left = requireNonNull(left, "left");
        this.right = requireNonNull(right, "right");
    }

    public Type getType()
    {
        return type;
    }

    public Expression getLeft()
    {
        return left;
    }

    public Expression getRight()
    {
        return right;
    }

    @Override
    public Class<?> getDataType()
    {
        return Boolean.class;
    }

    @Override
    public boolean isConstant()
    {
        return left.isConstant() && right.isConstant();
    }

    @Override
    public Expression fold()
    {
        // https://en.wikipedia.org/wiki/Null_(SQL)#Comparisons_with_NULL_and_the_three-valued_logic_.283VL.29
        // AND - false if either side is false or null
        if (type == Type.AND)
        {
            if (isFalse(left) || isFalse(right))
            {
                return FALSE_LITERAL;
            }
            else if (isTrue(left))
            {
                return right;
            }
            else if (isTrue(right))
            {
                return left;
            }
        }
        // OR  3vl True if either side is true or null */
        else if (isFalse(left))
        {
            return right;
        }
        else if (isFalse(right))
        {
            return left;
        }
        else if (isTrue(left) || isTrue(right))
        {
            return TRUE_LITERAL;
        }

        return this;
    }

    //CSOFF
    @Override
    public Object eval(ExecutionContext context)
    //CSON
    {
        Object lr = left.eval(context);
        if (type == Type.AND)
        {
            /* False if either side is false or null */
            if (lr != null && !(Boolean) lr)
            {
                return false;
            }

            Object rr = right.eval(context);

            if (rr != null && !(Boolean) rr)
            {
                return false;
            }

            if (rr == null || lr == null)
            {
                return null;
            }

            return true;
        }

        /* OR 3vl
            True if either side is true or null */
        if (lr != null && (Boolean) lr)
        {
            return true;
        }

        Object rr = right.eval(context);

        if (rr != null && (Boolean) rr)
        {
            return true;
        }

        if (lr == null || rr == null)
        {
            return null;
        }

        return false;
    }

    @Override
    public boolean isCodeGenSupported()
    {
        return left.isCodeGenSupported() && right.isCodeGenSupported();
    }

    @Override
    public ExpressionCode generateCode(CodeGeneratorContext context)
    {
        ExpressionCode code = context.getCode();

        ExpressionCode leftCode = left.generateCode(context);
        ExpressionCode rightCode = right.generateCode(context);

        if (type == LogicalBinaryExpression.Type.AND)
        {
            /* False if either side is false or null */
            String template = "%s"
                + "Boolean %s = false;\n"
                + "if (%s != null && (Boolean) %s)\n"
                + "{\n"
                + "  %s"
                + "  %s = %s != null && (Boolean) %s;\n"
                + "}\n";

            code.setCode(String.format(template,
                    leftCode.getCode(),
                    code.getResVar(),
                    leftCode.getResVar(), leftCode.getResVar(),
                    rightCode.getCode(),
                    code.getResVar(), rightCode.getResVar(), rightCode.getResVar()));
        }
        else    // OR
        {
            // true if left or right is true no matter of null
            String template = "%s"
                + "Boolean %s = true;\n"
                + "if (%s == null || !(Boolean) %s)\n"
                + "{\n"
                + "  %s"
                + "  %s = %s != null && (Boolean) %s;\n"
                + "}\n";

            code.setCode(String.format(template,
                    leftCode.getCode(),
                    code.getResVar(),
                    leftCode.getResVar(), leftCode.getResVar(),
                    rightCode.getCode(),
                    code.getResVar(), rightCode.getResVar(), rightCode.getResVar()));
        }

        return code;
    }

    @Override
    public <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    private boolean isTrue(Expression expression)
    {
        return TRUE_LITERAL.equals(expression);
    }

    private boolean isFalse(Expression expression)
    {
        return FALSE_LITERAL.equals(expression);
    }

    @Override
    public String toString()
    {
        return left.toString() + " " + type + " " + right.toString();
    }

    @Override
    public int hashCode()
    {
        //CSOFF
        int hashCode = 17;
        hashCode = hashCode * 37 + left.hashCode();
        hashCode = hashCode * 37 + right.hashCode();
        return hashCode;
        //CSON
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof LogicalBinaryExpression)
        {
            LogicalBinaryExpression that = (LogicalBinaryExpression) obj;
            return left.equals(that.left)
                && right.equals(that.right)
                && type == that.type;
        }
        return false;
    }

    /** Type */
    public enum Type
    {
        AND, OR;
    }
}
