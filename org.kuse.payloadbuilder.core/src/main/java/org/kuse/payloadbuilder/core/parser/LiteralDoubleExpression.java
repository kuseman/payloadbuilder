package org.kuse.payloadbuilder.core.parser;

import org.kuse.payloadbuilder.core.codegen.CodeGeneratorContext;
import org.kuse.payloadbuilder.core.codegen.ExpressionCode;

/** Literal double */
public class LiteralDoubleExpression extends LiteralExpression
{
    private final double value;

    LiteralDoubleExpression(String value)
    {
        this(Double.parseDouble(value));
    }

    LiteralDoubleExpression(double value)
    {
        super(value);
        this.value = value;
    }

    public double getValue()
    {
        return value;
    }

    @Override
    public ExpressionCode generateCode(CodeGeneratorContext context)
    {
        ExpressionCode code = context.getCode();
        String template = "Double %s = %sd;\n";
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
        return Double.toString(value);
    }
}
