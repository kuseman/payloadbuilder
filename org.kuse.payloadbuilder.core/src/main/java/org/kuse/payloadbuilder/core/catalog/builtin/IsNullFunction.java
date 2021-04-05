package org.kuse.payloadbuilder.core.catalog.builtin;

import static java.util.Arrays.asList;

import java.util.List;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.codegen.CodeGeneratorContext;
import org.kuse.payloadbuilder.core.codegen.ExpressionCode;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Returns first item if not null else second item */
class IsNullFunction extends ScalarFunctionInfo
{
    IsNullFunction(Catalog catalog)
    {
        super(catalog, "isnull");
    }

    @Override
    public String getDescription()
    {
        return "Returns first non null value of provided arguments. " + System.lineSeparator()
            + "Ex. isnull(expression, expression)" + System.lineSeparator()
            + "If both arguments yield null, null is returned.";
    }

    @Override
    public List<Class<? extends Expression>> getInputTypes()
    {
        return asList(Expression.class, Expression.class);
    }

    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        Object obj = arguments.get(0).eval(context);
        if (obj != null)
        {
            return obj;
        }

        return arguments.get(1).eval(context);
    }

    @Override
    public boolean isCodeGenSupported(List<Expression> arguments)
    {
        return arguments.get(0).isCodeGenSupported() && arguments.get(1).isCodeGenSupported();
    }

    @Override
    public ExpressionCode generateCode(CodeGeneratorContext context, List<Expression> arguments)
    {
        ExpressionCode code = context.getCode();

        ExpressionCode arg0Code = arguments.get(0).generateCode(context);
        ExpressionCode arg1Code = arguments.get(1).generateCode(context);

        String template = "// ISNULL\n"
            + "%s"
            + "Object %s = %s;\n"
            + "if (%s == null)\n"
            + "{\n"
            + "  %s"
            + "  %s = %s;\n"
            + "}\n";
        code.setCode(String.format(template,
                arg0Code.getCode(),
                code.getResVar(), arg0Code.getResVar(),
                code.getResVar(),
                arg1Code.getCode(),
                code.getResVar(), arg1Code.getResVar()));
        return code;
    }
}
