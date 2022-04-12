package se.kuseman.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

import se.kuseman.payloadbuilder.api.codegen.CodeGeneratorContext;
import se.kuseman.payloadbuilder.api.codegen.ExpressionCode;

/** Literal string */
public class LiteralStringExpression extends LiteralExpression
{
    private final String value;

    public LiteralStringExpression(String value)
    {
        super(value);
        this.value = requireNonNull(value);
    }

    @Override
    public ExpressionCode generateCode(CodeGeneratorContext context)
    {
        ExpressionCode code = context.getExpressionCode();
        String template = "String %s = \"%s\";\n" + "boolean %s = false;\n";
        code.setCode(String.format(template, code.getResVar(), value, code.getNullVar()));
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
