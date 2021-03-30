package org.kuse.payloadbuilder.core.catalog.builtin;

import static java.util.stream.Collectors.joining;

import java.util.ArrayList;
import java.util.List;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.codegen.CodeGeneratorContext;
import org.kuse.payloadbuilder.core.codegen.ExpressionCode;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Concat function. Concatenates all arguments into a string */
class ConcatFunction extends ScalarFunctionInfo
{
    ConcatFunction(Catalog catalog)
    {
        super(catalog, "concat");
    }

    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        int size = arguments.size();
        if (size <= 0)
        {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        for (Expression arg : arguments)
        {
            Object object = arg.eval(context);
            if (object != null)
            {
                sb.append(object);
            }
        }
        return sb.toString();
    }

    @Override
    public ExpressionCode generateCode(
            CodeGeneratorContext context,
            ExpressionCode parentCode,
            List<Expression> arguments)
    {
        ExpressionCode code = ExpressionCode.code(context);
        code.addImport("org.kuse.payloadbuilder.core.utils.ObjectUtils");

        List<String> argsResVars = new ArrayList<>(arguments.size());
        StringBuilder sb = new StringBuilder();
        for (Expression arg : arguments)
        {
            ExpressionCode argCode = arg.generateCode(context, parentCode);
            code.addImports(argCode.getImports());
            argsResVars.add(argCode.getResVar());
            sb.append(argCode.getCode());
        }

        // TODO: Fix iterator concating even if arguments are object

        String template = "%s"
            + "boolean %s = false;\n"
            + "Object  %s = ObjectUtils.concat(%s);\n";

        code.setCode(String.format(template,
                sb.toString(),
                code.getIsNull(),
                code.getResVar(),
                argsResVars.stream().collect(joining(","))));
        return code;
    }
}
