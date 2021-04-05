package org.kuse.payloadbuilder.core.catalog.builtin;

import static java.util.Arrays.asList;

import java.util.List;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.codegen.CodeGeneratorContext;
import org.kuse.payloadbuilder.core.codegen.ExpressionCode;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.utils.ObjectUtils;

/** Function listOf. Creates a list of provided arguments */
class ContainsFunction extends ScalarFunctionInfo
{
    ContainsFunction(Catalog catalog)
    {
        super(catalog, "contains");
    }

    @Override
    public String getDescription()
    {
        return "Checks if provided collection contains value argument" + System.lineSeparator()
            + "ie. contains(<collection expression>, <value expression>)";
    }

    @Override
    public List<Class<? extends Expression>> getInputTypes()
    {
        return asList(Expression.class, Expression.class);
    }

    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        // Function has a declared getInputTypes method that guards against wrong argument count
        Object arg0 = arguments.get(0).eval(context);
        Object arg1 = arguments.get(1).eval(context);

        return ObjectUtils.contains(arg0, arg1);
    }

    @Override
    public boolean isCodeGenSupported(List<Expression> arguments)
    {
        return arguments.stream().allMatch(Expression::isCodeGenSupported);
    }

    @Override
    public ExpressionCode generateCode(CodeGeneratorContext context, List<Expression> arguments)
    {
        ExpressionCode code = context.getCode();
        context.addImport("org.kuse.payloadbuilder.core.utils.ObjectUtils");

        ExpressionCode arg0Code = arguments.get(0).generateCode(context);
        ExpressionCode arg1Code = arguments.get(1).generateCode(context);

        String template = "%s%s"
            + "Boolean %s = ObjectUtils.contains(%s, %s);\n";
        code.setCode(String.format(template,
                arg0Code.getCode(),
                arg1Code.getCode(),
                code.getResVar(), arg0Code.getResVar(), arg1Code.getResVar()));
        return code;
    }
}
