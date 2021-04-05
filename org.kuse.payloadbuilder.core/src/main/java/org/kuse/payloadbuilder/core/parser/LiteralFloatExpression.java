package org.kuse.payloadbuilder.core.parser;

import org.kuse.payloadbuilder.core.codegen.CodeGeneratorContext;
import org.kuse.payloadbuilder.core.codegen.ExpressionCode;

/** Literal float */
public class LiteralFloatExpression extends LiteralExpression
{
    private final float value;

    LiteralFloatExpression(String value)
    {
        this(Float.parseFloat(value));
    }

    LiteralFloatExpression(float value)
    {
        super(value);
        this.value = value;
    }

    public float getValue()
    {
        return value;
    }

    @Override
    public ExpressionCode generateCode(CodeGeneratorContext context)
    {
        ExpressionCode code = context.getCode();
        String template = "Float %s = %sf;\n";
        code.setCode(String.format(template, code.getResVar(), value));
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
        return Float.toString(value);
    }
}
