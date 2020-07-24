package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

import org.kuse.payloadbuilder.core.codegen.CodeGeneratorContext;
import org.kuse.payloadbuilder.core.codegen.ExpressionCode;

public class LiteralStringExpression extends LiteralExpression
{
    private final String value;

    public LiteralStringExpression(String value)
    {
        super(value);
        this.value = requireNonNull(value);
    }
    
    @Override
    public ExpressionCode generateCode(CodeGeneratorContext context, ExpressionCode parentCode)
    {
        ExpressionCode code = ExpressionCode.code(context);
        String template = "String %s = \"%s\";\n"
            + "boolean %s = false;\n";
        code.setCode(String.format(template, code.getResVar(), value, code.getIsNull()));
        return code;
    }
    
    @Override
    public <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    public String getValue()
    {
        return value;
    }

    @Override
    public String toString()
    {
        return "'" + value + "'";
    }
}
