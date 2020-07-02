package com.viskan.payloadbuilder.catalog.builtin;

import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.ScalarFunctionInfo;
import com.viskan.payloadbuilder.codegen.CodeGeneratorContext;
import com.viskan.payloadbuilder.codegen.ExpressionCode;
import com.viskan.payloadbuilder.parser.ExecutionContext;
import com.viskan.payloadbuilder.parser.Expression;

import static java.util.Collections.emptyList;

import java.util.List;

/** Now function, returns the current unix epoch */
class NowFunction extends ScalarFunctionInfo
{
    NowFunction(Catalog catalog)
    {
        super(catalog, "now", Type.SCALAR);
    }

    @Override
    public List<Class<? extends Expression>> getInputTypes()
    {
        return emptyList();
    }

    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        return context.getNow();
    }

    @Override
    public ExpressionCode generateCode(CodeGeneratorContext context, ExpressionCode parentCode, List<Expression> arguments)
    {
        ExpressionCode code = ExpressionCode.code(context);
        code.setCode(String.format("boolean %s = false;\n"
            + "long %s = %sL;\n", code.getIsNull(), code.getResVar(), context.getNow()));
        return code;
    }

    @Override
    public boolean isNullable()
    {
        return false;
    }

    @Override
    public Class<?> getDataType()
    {
        return long.class;
    }
}
