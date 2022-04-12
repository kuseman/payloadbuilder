package se.kuseman.payloadbuilder.core.parser;

import se.kuseman.payloadbuilder.api.TableMeta.DataType;
import se.kuseman.payloadbuilder.api.codegen.CodeGeneratorContext;
import se.kuseman.payloadbuilder.api.codegen.ExpressionCode;

/** Literal long */
public class LiteralLongExpression extends LiteralExpression
{
    private final long value;

    public LiteralLongExpression(String value)
    {
        this(Long.parseLong(value));
    }

    LiteralLongExpression(long value)
    {
        super(value);
        this.value = value;
    }

    @Override
    public DataType getDataType()
    {
        return DataType.LONG;
    }

    @Override
    public ExpressionCode generateCode(CodeGeneratorContext context)
    {
        ExpressionCode code = context.getExpressionCode();
        String template = "long %s = %sl;\n" + "boolean %s = false;\n";
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
        return Long.toString(value);
    }
}
