package com.viskan.payloadbuilder.parser.tree;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.codegen.CodeGeneratorContext;
import com.viskan.payloadbuilder.codegen.ExpressionCode;
import com.viskan.payloadbuilder.evaluation.EvaluationContext;

import static java.util.Objects.requireNonNull;

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
    public boolean isNullable()
    {
        return left.isNullable() || right.isNullable();
    }
    
    @Override
    public Object eval(EvaluationContext evaluationContext, Row row)
    {
        Object lr = left.eval(evaluationContext, row);
        if (type == Type.AND)
        {
            /* False if either side is false or null */
            if (lr != null && !(Boolean) lr)
            {
                return false;
            }
            
            Object rr = right.eval(evaluationContext, row);

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
        
        Object rr = right.eval(evaluationContext, row);
        
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
    public ExpressionCode generateCode(CodeGeneratorContext context, ExpressionCode parentCode)
    {
        ExpressionCode leftCode = left.generateCode(context, parentCode);
        ExpressionCode rightCode = right.generateCode(context, parentCode);

        ExpressionCode code = ExpressionCode.code(context);

        if (type == LogicalBinaryExpression.Type.AND)
        {
            // false if left or right is false no matter of null
            if (!left.isNullable() && !right.isNullable())
            {
                String template = "%s"
                    + "boolean %s = false;\n"
                    + "boolean %s = false;\n"
                    + "if (%s)\n"
                    + "{\n"
                    + "  %s"
                    + "  %s = %s;\n"
                    + "}\n";

                code.setCode(String.format(template,
                        leftCode.getCode(),
                        code.getResVar(),
                        code.getIsNull(),
                        leftCode.getResVar(),
                        rightCode.getCode(),
                        code.getResVar(), rightCode.getResVar()));
            }
            else
            {
                boolean addLeftCast = !Boolean.class.isAssignableFrom(left.getDataType());
                boolean addRightCast = !Boolean.class.isAssignableFrom(right.getDataType());

                String castedLeftVar = addLeftCast ? "(Boolean)" + leftCode.getResVar() : leftCode.getResVar();
                String castedRightVar = addRightCast ? "(Boolean)" + rightCode.getResVar() : rightCode.getResVar();

                String template = "%s"
                    + "boolean %s = false;\n"
                    + "boolean %s = false;\n"
                    + "if (!%s && !%s) {}\n"
                    + "else\n"
                    + "{\n"
                    + "  %s"
                    + "  if (!%s && !%s) {}\n"
                    + "  else if (!%s && !%s)\n"
                    + "  {\n"
                    + "    %s = true;\n"
                    + "  }\n"
                    + "  else\n"
                    + "  {\n"
                    + "    %s = true;\n"
                    + "  }\n"
                    + "}\n";

                code.setCode(String.format(template,
                        leftCode.getCode(),
                        code.getResVar(),
                        code.getIsNull(),
                        leftCode.getIsNull(), castedLeftVar,
                        rightCode.getCode(),
                        rightCode.getIsNull(), castedRightVar,
                        leftCode.getIsNull(), rightCode.getIsNull(),
                        code.getResVar(),
                        code.getIsNull()));
            }
        }
        else    // OR
        {
            // true if left or right is true no matter of null
            if (!left.isNullable() && !right.isNullable())
            {
                String template = "%s"
                    + "boolean %s = true;\n"
                    + "boolean %s = false;\n"
                    + "if (!%s)\n"
                    + "{\n"
                    + "  %s"
                    + "  %s = %s;\n"
                    + "}\n";

                code.setCode(String.format(template,
                        leftCode.getCode(),
                        code.getResVar(),
                        code.getIsNull(),
                        leftCode.getResVar(),
                        rightCode.getCode(),
                        code.getResVar(), rightCode.getResVar()));
            }
            else
            {
                boolean addLeftCast = !Boolean.class.isAssignableFrom(left.getDataType());
                boolean addRightCast = !Boolean.class.isAssignableFrom(right.getDataType());

                String castedLeftVar = addLeftCast ? "(Boolean)" + leftCode.getResVar() : leftCode.getResVar();
                String castedRightVar = addRightCast ? "(Boolean)" + rightCode.getResVar() : rightCode.getResVar();

                String template = "%s"
                    + "boolean %s = true;\n"
                    + "boolean %s = false;\n"
                    + "if (!%s && %s){}\n"
                    + "else\n"
                    + "{\n"
                    + "  %s"
                    + "  if (!%s && %s){}\n"
                    + "  else if (!%s && !%s)\n"
                    + "  {\n"
                    + "    %s = false;\n"
                    + "  }\n"
                    + "  else\n"
                    + "  {\n"
                    + "    %s = true;\n"
                    + "  }\n"
                    + "}\n";

                code.setCode(String.format(template,
                        leftCode.getCode(),
                        code.getResVar(),
                        code.getIsNull(),
                        leftCode.getIsNull(), castedLeftVar,
                        rightCode.getCode(),
                        rightCode.getIsNull(), castedRightVar,
                        leftCode.getIsNull(), rightCode.getIsNull(),
                        code.getResVar(),
                        code.getIsNull()));
            }
        }

        return code;    
    }
    
    @Override
    public <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }
    
    @Override
    public String toString()
    {
        return left.toString() + " " + type + " " + right.toString();
    }
    
    public enum Type 
    {
        AND,OR;
    }
}
