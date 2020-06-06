package com.viskan.payloadbuilder.parser.tree;

import com.viskan.payloadbuilder.codegen.CodeGeneratorContext;
import com.viskan.payloadbuilder.codegen.ExpressionCode;

public class LiteralDecimalExpression extends LiteralExpression
{
    private final double value;

    public LiteralDecimalExpression(String value)
    {
        this(Double.parseDouble(value));
    }
    
    LiteralDecimalExpression(double value)
    {
        super(value);
        this.value = value;
    }
    
    public double getValue()
    {
        return value;
    }
    
    @Override
    public ExpressionCode generateCode(CodeGeneratorContext context, ExpressionCode parentCode)
    {
        ExpressionCode code = ExpressionCode.code(context);
        String template = "double %s = %s;\n"
            + "boolean %s = false;\n";
        code.setCode(String.format(template, code.getResVar(), value, code.getIsNull()));
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
        return Double.toString(value);
    }
}
