package org.kuse.payloadbuilder.core.catalog.builtin;

import java.util.List;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.operator.EvalUtils;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
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
                sb.append(EvalUtils.unwrap(context, object));
            }
        }
        return sb.toString();
    }

    //    @Override
    //    public ExpressionCode generateCode(
    //            CodeGeneratorContext context,
    //            ExpressionCode parentCode,
    //            List<Expression> arguments)
    //    {
    //        ExpressionCode code = ExpressionCode.code(context, parentCode);
    //        context.addImport("org.kuse.payloadbuilder.core.utils.ObjectUtils");
    //
    //        List<String> argsResVars = new ArrayList<>(arguments.size());
    //        StringBuilder sb = new StringBuilder();
    //        for (Expression arg : arguments)
    //        {
    //            ExpressionCode argCode = arg.generateCode(context, parentCode);
    //            argsResVars.add(argCode.getResVar());
    //            sb.append(argCode.getCode());
    //        }
    //
    //        // TODO: Fix iterator concating even if arguments are object
    //
    //        String template = "%s"
    //            + "Object %s = ObjectUtils.concat(%s);\n";
    //
    //        code.setCode(String.format(template,
    //                sb.toString(),
    //                code.getIsNull(),
    //                code.getResVar(),
    //                argsResVars.stream().collect(joining(","))));
    //        return code;
    //    }
}
