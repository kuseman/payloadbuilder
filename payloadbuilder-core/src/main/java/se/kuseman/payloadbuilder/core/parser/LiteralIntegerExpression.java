package se.kuseman.payloadbuilder.core.parser;

import se.kuseman.payloadbuilder.api.TableMeta.DataType;
import se.kuseman.payloadbuilder.api.codegen.CodeGeneratorContext;
import se.kuseman.payloadbuilder.api.codegen.ExpressionCode;

/** Literal integer */
public class LiteralIntegerExpression extends LiteralExpression
{
    private final int value;

    LiteralIntegerExpression(String value)
    {
        this(Integer.parseInt(value));
    }

    LiteralIntegerExpression(int value)
    {
        super(value);
        this.value = value;
    }

    public int getValue()
    {
        return value;
    }

    @Override
    public DataType getDataType()
    {
        return DataType.INT;
    }

    @Override
    public ExpressionCode generateCode(CodeGeneratorContext context)
    {
        ExpressionCode code = context.getExpressionCode();
        String template = "int %s = %s;\n" + "boolean %s = false;\n";
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
        return Integer.toString(value);
    }
}
