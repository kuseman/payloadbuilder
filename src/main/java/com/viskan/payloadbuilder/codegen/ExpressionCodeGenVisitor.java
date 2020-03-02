package com.viskan.payloadbuilder.codegen;

import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.parser.tree.ArithmeticBinaryExpression;
import com.viskan.payloadbuilder.parser.tree.ArithmeticUnaryExpression;
import com.viskan.payloadbuilder.parser.tree.ComparisonExpression;
import com.viskan.payloadbuilder.parser.tree.DereferenceExpression;
import com.viskan.payloadbuilder.parser.tree.Expression;
import com.viskan.payloadbuilder.parser.tree.InExpression;
import com.viskan.payloadbuilder.parser.tree.LiteralBooleanExpression;
import com.viskan.payloadbuilder.parser.tree.LiteralDecimalExpression;
import com.viskan.payloadbuilder.parser.tree.LiteralNullExpression;
import com.viskan.payloadbuilder.parser.tree.LiteralNumericExpression;
import com.viskan.payloadbuilder.parser.tree.LiteralStringExpression;
import com.viskan.payloadbuilder.parser.tree.LogicalBinaryExpression;
import com.viskan.payloadbuilder.parser.tree.LogicalNotExpression;
import com.viskan.payloadbuilder.parser.tree.NestedExpression;
import com.viskan.payloadbuilder.parser.tree.NullPredicateExpression;
import com.viskan.payloadbuilder.parser.tree.QualifiedFunctionCallExpression;
import com.viskan.payloadbuilder.parser.tree.QualifiedReferenceExpression;
import com.viskan.payloadbuilder.parser.tree.TreeVisitor;

import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.List;
import java.util.Objects;

/** Generates Java code for expressions. Used by janino */
class ExpressionCodeGenVisitor implements TreeVisitor<ExpressionCode, CodeGenratorContext>
{
    /** Generate code for expression */
    ExpressionCode generate(Expression expression, TableAlias tableAlias, boolean biPredicate, boolean pretty)
    {
        CodeGenratorContext ctx = new CodeGenratorContext();
        ctx.biPredicate = biPredicate;
        ctx.tableAlias = tableAlias;
        ctx.pretty = pretty;
        return expression.accept(this, ctx);
    }

    @Override
    public ExpressionCode visit(LiteralNullExpression expression, CodeGenratorContext context)
    {
        ExpressionCode code = ExpressionCode.code(context);
        String template = "Object %s = null;\n"
            + "boolean %s = true;\n";
        code.code = String.format(template, code.resVar, code.isNull);
        return code;
    }

    @Override
    public ExpressionCode visit(LiteralBooleanExpression expression, CodeGenratorContext context)
    {
        ExpressionCode code = ExpressionCode.code(context);
        String template = "boolean %s = %s;\n"
            + "boolean %s = false;\n";
        code.code = String.format(template, code.resVar, expression.getValue(), code.isNull);
        return code;
    }

    @Override
    public ExpressionCode visit(LiteralNumericExpression expression, CodeGenratorContext context)
    {
        ExpressionCode code = ExpressionCode.code(context);
        String template = "long %s = %s;\n"
            + "boolean %s = false;\n";
        code.code = String.format(template, code.resVar, expression.getValue(), code.isNull);
        return code;
    }

    @Override
    public ExpressionCode visit(LiteralDecimalExpression expression, CodeGenratorContext context)
    {
        ExpressionCode code = ExpressionCode.code(context);
        String template = "double %s = %s;\n"
            + "boolean %s = false;\n";
        code.code = String.format(template, code.resVar, expression.getValue(), code.isNull);
        return code;
    }

    @Override
    public ExpressionCode visit(LiteralStringExpression expression, CodeGenratorContext context)
    {
        ExpressionCode code = ExpressionCode.code(context);
        String template = "String %s = \"%s\";\n"
            + "boolean %s = false;\n";
        code.code = String.format(template, code.resVar, expression.getValue(), code.isNull);
        return code;
    }

    @Override
    public ExpressionCode visit(ComparisonExpression expression, CodeGenratorContext context)
    {
        Expression left = expression.getLeft();
        Expression right = expression.getRight();
        ExpressionCode leftCode = left.accept(this, context);
        ExpressionCode rightCode = right.accept(this, context);

        ExpressionCode code = ExpressionCode.code(context);
        code.code = "// " + expression.toString() + "\n";
        code.code += leftCode.code;
        code.code += rightCode.code;

        // TODO: nullable for faster comparisons
        //       Datatype checks, boolean
        String cmpOp = null;
        switch (expression.getType())
        {
            case EQUAL:
                cmpOp = "EQUAL";
                break;
            case NOT_EQUAL:
                cmpOp = "NOT_EQUAL";
                break;
            case GREATER_THAN:
                cmpOp = "GREATER_THAN";
                break;
            case GREATER_THAN_EQUAL:
                cmpOp = "GREATER_THAN_EQUAL";
                break;
            case LESS_THAN:
                cmpOp = "LESS_THAN";
                break;
            case LESS_THAN_EQUAL:
                cmpOp = "LESS_THAN_EQUAL";
                break;
        }

        String resultType = left.isNullable() || right.isNullable() ? "Boolean" : "boolean";
        code.code += String.format(
                "boolean %s = false;\n"
                    + "%s %s = compare(%s, %s, %s);\n",
                code.isNull, resultType, code.resVar, leftCode.resVar, rightCode.resVar, cmpOp);

        return code;
    }
    
    @Override
    public ExpressionCode visit(ArithmeticUnaryExpression expression, CodeGenratorContext context)
    {
        return null;
    }

    @Override
    public ExpressionCode visit(ArithmeticBinaryExpression expression, CodeGenratorContext context)
    {
        Expression left = expression.getLeft();
        Expression right = expression.getRight();
        ExpressionCode leftCode = left.accept(this, context);
        ExpressionCode rightCode = right.accept(this, context);
        ExpressionCode code = ExpressionCode.code(context);

        String method = null;
        switch (expression.getType())
        {
            case ADD:
                method = "add";
                break;
            case SUBTRACT:
                method = "subtract";
                break;
            case MULTIPLY:
                method = "multiply";
                break;
            case DIVIDE:
                method = "divide";
                break;
            case MODULUS:
                method = "modulo";
                break;
        }

        String template = "%s"
            + "%s"
            + "Object %s = null;\n"
            + "boolean %s = true;\n"
            + "if (!%s && !%s)\n"
            + "{\n"
            + "  %s = %s(%s, %s);\n"
            + "  %s = false;\n"
            + "}\n";

        code.code = String.format(template,
                leftCode.code,
                rightCode.code,
                code.resVar,
                code.isNull,
                leftCode.isNull, rightCode.isNull,
                code.resVar, method, leftCode.resVar, rightCode.resVar,
                code.isNull);

        return code;

    }

    @Override
    public ExpressionCode visit(LogicalBinaryExpression expression, CodeGenratorContext context)
    {
        Expression left = expression.getLeft();
        Expression right = expression.getRight();
        ExpressionCode leftCode = left.accept(this, context);
        ExpressionCode rightCode = right.accept(this, context);

        ExpressionCode code = ExpressionCode.code(context);

        if (expression.getType() == LogicalBinaryExpression.Type.AND)
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

                code.code = String.format(template,
                        leftCode.code,
                        code.resVar,
                        code.isNull,
                        leftCode.resVar,
                        rightCode.code,
                        code.resVar, rightCode.resVar);
            }
            else
            {
                boolean addLeftCast = !Boolean.class.isAssignableFrom(left.getDataType());
                boolean addRightCast = !Boolean.class.isAssignableFrom(right.getDataType());

                String castedLeftVar = addLeftCast ? "(Boolean)" + leftCode.resVar : leftCode.resVar;
                String castedRightVar = addRightCast ? "(Boolean)" + rightCode.resVar : rightCode.resVar;

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

                code.code += String.format(template,
                        leftCode.code,
                        code.resVar,
                        code.isNull,
                        leftCode.isNull, castedLeftVar,
                        rightCode.code,
                        rightCode.isNull, castedRightVar,
                        leftCode.isNull, rightCode.isNull,
                        code.resVar,
                        code.isNull);
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

                code.code = String.format(template,
                        leftCode.code,
                        code.resVar,
                        code.isNull,
                        leftCode.resVar,
                        rightCode.code,
                        code.resVar, rightCode.resVar);
            }
            else
            {
                boolean addLeftCast = !Boolean.class.isAssignableFrom(left.getDataType());
                boolean addRightCast = !Boolean.class.isAssignableFrom(right.getDataType());

                String castedLeftVar = addLeftCast ? "(Boolean)" + leftCode.resVar : leftCode.resVar;
                String castedRightVar = addRightCast ? "(Boolean)" + rightCode.resVar : rightCode.resVar;

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

                code.code += String.format(template,
                        leftCode.code,
                        code.resVar,
                        code.isNull,
                        leftCode.isNull, castedLeftVar,
                        rightCode.code,
                        rightCode.isNull, castedRightVar,
                        leftCode.isNull, rightCode.isNull,
                        code.resVar,
                        code.isNull);
            }
        }

        return code;
    }

    @Override
    public ExpressionCode visit(LogicalNotExpression expression, CodeGenratorContext context)
    {
        ExpressionCode childCode = expression.getExpression().accept(this, context);

        if (expression.isNullable())
        {
            boolean addCast = !Boolean.class.isAssignableFrom(expression.getDataType());

            String template = "%s\n"
                + "if (!%s)\n"
                + "{\n"
                + "  %s = !%s%s;\n"
                + "}\n";

            childCode.code = String.format(template,
                    childCode.code,
                    childCode.isNull,
                    childCode.resVar,
                    addCast ? "(Boolean)" : "", childCode.resVar);
        }
        else
        {
            String template = "%s\n"
                + "%s = !%s;\n";
            childCode.code = String.format(template,
                    childCode.code,
                    childCode.resVar, childCode.resVar);
        }

        childCode.code = "// NOT\n" + childCode.code;
        return childCode;
    }

    @Override
    public ExpressionCode visit(NestedExpression nestedExpression, CodeGenratorContext context)
    {
        return nestedExpression.getExpression().accept(this, context);
    }

    @Override
    public ExpressionCode visit(InExpression expression, CodeGenratorContext context)
    {
        ExpressionCode childCode = expression.getExpression().accept(this, context);
        ExpressionCode code = ExpressionCode.code(context);

        int size = expression.getArguments().size();
        // Generate code for
        StringBuilder sb = new StringBuilder();
        sb.append(childCode.code);

        sb.append("boolean ").append(code.resVar).append(" = false;\n");
        sb.append("boolean ").append(code.isNull).append(" = ").append(childCode.isNull).append(";\n");

        String template = "if (!%s && !%s)\n"
            + "{\n"
            + "  %s\n"
            + "  if (!%s)\n"
            + "  {\n"
            + "    %s = inValue(%s, %s);\n"
            + "    %s = false;\n"
            + "  }\n"
            + "}\n";

        for (int i = 0; i < size; i++)
        {
            Expression arg = expression.getArguments().get(i);
            ExpressionCode argCode = arg.accept(this, context);
            sb.append(String.format(template,
                    code.isNull, code.resVar,
                    argCode.code,
                    argCode.isNull,
                    code.resVar, childCode.resVar, argCode.resVar,
                    code.isNull));
        }

        code.code = sb.toString();
        return code;
    }

    @Override
    public ExpressionCode visit(NullPredicateExpression expression, CodeGenratorContext context)
    {
        ExpressionCode childCode = expression.getExpression().accept(this, context);
        ExpressionCode code = ExpressionCode.code(context);

        /*
         * Object res0;
         * boolean isNull0;
         *
         * boolean res1 = !isNull0;
         * boolean isNull1 = false;
         *
         */

        String template = "%s"
            + "boolean %s = %s%s;\n"
            + "boolean %s = false;\n";

        code.code = String.format(template,
                childCode.code,
                code.resVar, expression.isNot() ? "!" : "", childCode.isNull,
                code.isNull);

        return code;
    }
    
    @Override
    public ExpressionCode visit(QualifiedFunctionCallExpression expression, CodeGenratorContext context)
    {
        /*
         * TODO: Needs function resolving
         */
        return ExpressionCode.code(context);
    }

    @Override
    public ExpressionCode visit(QualifiedReferenceExpression expression, CodeGenratorContext context)
    {
        // TODO: Fix column reference
        //       1. Cache ordinal
        //       2. Nested path

        ExpressionCode code = ExpressionCode.code(context);

        String rowName = "row";
        List<String> parts = expression.getQname().getParts();
        String column = expression.getQname().getParts().get(0);

        if (context.biPredicate)
        {
            String alias = expression.getQname().getAlias();
            // Blank alias or inner alias => inner row else outer
            rowName = isBlank(alias) || Objects.equals(alias, context.tableAlias.getAlias()) ? "r_in" : "r_out";
            if (parts.size() > 1)
            {
                // Only one nest level supported for now
                column = parts.get(1);
            }

        }

        //        String inputVariableName = rowName + "_" + column;
        //        if (context.addedVars.add(inputVariableName))
        //        {
        //            String stm = "Input " + inputVariableName + " = new Input(\"" + column + "\");" + System.lineSeparator();
        //            context.appendVariable(stm);
        //        }

        //        if (context.desiredType != null)
        //        {
        //            if (Number.class.isAssignableFrom(context.desiredType))
        //            {
        //                context.appendBody(inputVariableName + ".getNumber(" + rowName + ")");
        //                return null;
        //            }
        //            else if (boolean.class.isAssignableFrom(context.desiredType))
        //            {
        //                context.appendBody(inputVariableName + ".getBoolean(" + rowName + ")");
        //                return null;
        //            }
        //        }

        code.code = String.format(
                "Object %s = %s.getObject(\"%s\");\n"
                    + "boolean %s = %s == null;\n",
                code.resVar, rowName, column, code.isNull, code.resVar);
        return code;
    }
    
    @Override
    public ExpressionCode visit(DereferenceExpression expression, CodeGenratorContext context)
    {
        /*
         * TODO: 
         */
        return ExpressionCode.code(context);
    }
}
