package com.viskan.payloadbuilder.catalog.builtin;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.ScalarFunctionInfo;
import com.viskan.payloadbuilder.codegen.CodeGeneratorContext;
import com.viskan.payloadbuilder.codegen.ExpressionCode;
import com.viskan.payloadbuilder.evaluation.EvaluationContext;
import com.viskan.payloadbuilder.parser.tree.Expression;
import com.viskan.payloadbuilder.utils.ObjectUtils;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/** Map function. Maps input into another form */
class ConcatFunction extends ScalarFunctionInfo
{
    ConcatFunction(Catalog catalog)
    {
        super(catalog, "concat", Type.SCALAR);
    }
    
    @Override
    public Set<TableAlias> resolveAlias(Set<TableAlias> parentAliases, List<Expression> arguments, Function<Expression, Set<TableAlias>> aliasResolver)
    {
        // Result of a concat is the result of all arguments aliases
        return arguments.stream().flatMap(a -> aliasResolver.apply(a).stream()).collect(toSet());
    }
    
    @Override
    public Object eval(EvaluationContext context, List<Expression> arguments, Row row)
    {
        int size = arguments.size();
        if (size <= 0)
        {
            return null;
        }
        
        Object[] args = new Object[size];
        for (int i=0;i<size;i++)
        {
            args[i] = arguments.get(i).eval(context, row);
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
        code.addImport("com.viskan.payloadbuilder.utils.ObjectUtils");
        
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
        
        String template =
                "%s"
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
