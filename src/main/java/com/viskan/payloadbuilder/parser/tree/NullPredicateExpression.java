package com.viskan.payloadbuilder.parser.tree;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.codegen.CodeGeneratorContext;
import com.viskan.payloadbuilder.codegen.ExpressionCode;
import com.viskan.payloadbuilder.evaluation.EvaluationContext;

import static java.util.Objects.requireNonNull;

/** Expression for is null / is not null */
public class NullPredicateExpression extends Expression
{
    private final Expression expression;
    private final boolean not;

    public NullPredicateExpression(Expression expression, boolean not)
    {
        this.expression = requireNonNull(expression);
        this.not = not;
    }

    public Expression getExpression()
    {
        return expression;
    }

    public boolean isNot()
    {
        return not;
    }
    
    @Override
    public Object eval(EvaluationContext evaluationContext, Row row)
    {
        Object result = expression.eval(evaluationContext, row);
        return not ? result != null : result == null;
    }
    
    @Override
    public ExpressionCode generateCode(CodeGeneratorContext context, ExpressionCode parentCode)
    {
        ExpressionCode childCode = expression.generateCode(context, parentCode);
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

        code.setCode(String.format(template,
                childCode.getCode(),
                code.getResVar(), isNot() ? "!" : "", childCode.getIsNull(),
                code.getIsNull()));

        return code;
    }

    @Override
    public <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public boolean isNullable()
    {
        return false;
    }

    @Override
    public Class<?> getDataType()
    {
        return boolean.class;
    }

    @Override
    public String toString()
    {
        return expression + " IS" + (not ? " NOT " : "") + "NULL";
    }
}
