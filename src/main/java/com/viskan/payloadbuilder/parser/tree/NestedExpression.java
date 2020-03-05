package com.viskan.payloadbuilder.parser.tree;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.codegen.CodeGeneratorContext;
import com.viskan.payloadbuilder.codegen.ExpressionCode;
import com.viskan.payloadbuilder.evaluation.EvaluationContext;

import static java.util.Objects.requireNonNull;

public class NestedExpression extends Expression
{
    private final Expression expression;
    
    public NestedExpression(Expression expression)
    {
        this.expression = requireNonNull(expression, "expression");
    }
    
    public Expression getExpression()
    {
        return expression;
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
    public Object eval(EvaluationContext evaluationContext, Row row)
    {
        return expression.eval(evaluationContext, row);
    }
    
    @Override
    public ExpressionCode generateCode(CodeGeneratorContext context, ExpressionCode parentCode)
    {
        return expression.generateCode(context, parentCode);
    }
    
    @Override
    public <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }
    
    @Override
    public String toString()
    {
        return "(" + expression.toString() + ")";
    }
}
