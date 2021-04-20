package org.kuse.payloadbuilder.core.parser;

import org.kuse.payloadbuilder.core.catalog.TableMeta.DataType;
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

    @Override
    public DataType getDataType()
    {
        return DataType.FLOAT;
    }

    @Override
    public ExpressionCode generateCode(CodeGeneratorContext context)
    {
        ExpressionCode code = context.getExpressionCode();
        String template = "float %s = %sf;\n"
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
        return Float.toString(value);
    }
}
