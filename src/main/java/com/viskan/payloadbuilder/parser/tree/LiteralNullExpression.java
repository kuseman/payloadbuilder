package com.viskan.payloadbuilder.parser.tree;

import com.viskan.payloadbuilder.codegen.CodeGenratorContext;
import com.viskan.payloadbuilder.codegen.ExpressionCode;

public class LiteralNullExpression extends LiteralExpression
{
    public static final LiteralNullExpression NULL_LITERAL = new LiteralNullExpression();
    
    private LiteralNullExpression()
    {
        super(null);
    }
    
    @Override
    public ExpressionCode generateCode(CodeGenratorContext context, ExpressionCode parentCode)
    {
        ExpressionCode code = ExpressionCode.code(context);
        String template = "Object %s = null;\n"
            + "boolean %s = true;\n";
        code.setCode(String.format(template, code.getResVar(), code.getIsNull()));
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
        return "null";
    }
}
