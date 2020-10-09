package org.kuse.payloadbuilder.core.catalog.builtin;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.codegen.CodeGeneratorContext;
import org.kuse.payloadbuilder.core.codegen.ExpressionCode;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.utils.ObjectUtils;

/** Map function. Maps input into another form */
class ConcatFunction extends ScalarFunctionInfo
{
    ConcatFunction(Catalog catalog)
    {
        super(catalog, "concat");
    }

    @Override
    public Set<TableAlias> resolveAlias(Set<TableAlias> parentAliases, List<Expression> arguments, Function<Expression, Set<TableAlias>> aliasResolver)
    {
        // Result of a concat is the result of all arguments aliases
        return arguments
                .stream()
                .flatMap(a ->
                {
                    Set<TableAlias> argAliases = aliasResolver.apply(a);
                    return argAliases == null ? Stream.empty() : argAliases.stream();
                })
                .collect(toSet());
    }

    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        int size = arguments.size();
        if (size <= 0)
        {
            return null;
        }

        Object[] args = new Object[size];
        for (int i = 0; i < size; i++)
        {
            args[i] = arguments.get(i).eval(context);
        }

        return ObjectUtils.concat(args);
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
