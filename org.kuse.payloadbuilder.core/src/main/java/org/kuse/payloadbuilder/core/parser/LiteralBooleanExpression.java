package org.kuse.payloadbuilder.core.parser;

import org.kuse.payloadbuilder.core.catalog.TableMeta.DataType;
import org.kuse.payloadbuilder.core.codegen.CodeGeneratorContext;
import org.kuse.payloadbuilder.core.codegen.ExpressionCode;

/** Literal boolean */
public class LiteralBooleanExpression extends LiteralExpression
{
    public static final LiteralBooleanExpression TRUE_LITERAL = new LiteralBooleanExpression(true);
    public static final LiteralBooleanExpression FALSE_LITERAL = new LiteralBooleanExpression(false);

    private final boolean value;

    private LiteralBooleanExpression(boolean value)
    {
        super(value);
        this.value = value;
    }

    boolean getValue()
    {
        return value;
    }

    @Override
    public DataType getDataType()
    {
        return DataType.BOOLEAN;
    }

    @Override
    public ExpressionCode generateCode(CodeGeneratorContext context)
    {
        ExpressionCode code = context.getExpressionCode();
        String template = "boolean %s = %s;\n"
            + "boolean %s = false;\n";
        code.setCode(String.format(template, code.getResVar(), value, code.getNullVar()));
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
        return Boolean.toString(value);
    }
}
