package com.viskan.payloadbuilder.parser.tree;

import com.viskan.payloadbuilder.codegen.CodeGenratorContext;
import com.viskan.payloadbuilder.codegen.ExpressionCode;

public class LiteralNumericExpression extends LiteralExpression
{
    private final long value;

    public LiteralNumericExpression(String value)
    {
        this(Long.parseLong(value));
    }
    
    private LiteralNumericExpression(long value)
    {
        super(value);
        this.value = value;
    }
    
    public long getValue()
    {
        return value;
    }

    @Override
    public ExpressionCode generateCode(CodeGenratorContext context, ExpressionCode parentCode)
    {
        ExpressionCode code = ExpressionCode.code(context);
        String template = "long %s = %s;\n"
            + "boolean %s = false;\n";
        code.setCode(String.format(template, code.getResVar(), value, code.getIsNull()));
        return code;
    }
    
    @Override
    public <TR, TC> TR accept(TreeVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }
    
    @Override
    public String toString()
    {
        return Long.toString(value);
    }

}
