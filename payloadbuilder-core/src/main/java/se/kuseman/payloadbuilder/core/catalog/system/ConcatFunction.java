package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.core.operator.EvalUtils;
import se.kuseman.payloadbuilder.core.operator.ExecutionContext;

/** Concat function. Concatenates all arguments into a string */
class ConcatFunction extends ScalarFunctionInfo
{
    ConcatFunction(Catalog catalog)
    {
        super(catalog, "concat");
    }

    @Override
    public Object eval(IExecutionContext ctx, String catalogAlias, List<? extends IExpression> arguments)
    {
        int size = arguments.size();
        if (size <= 0)
        {
            return null;
        }

        ExecutionContext context = (ExecutionContext) ctx;
        StringBuilder sb = new StringBuilder();
        for (IExpression arg : arguments)
        {
            Object object = arg.eval(context);
            if (object != null)
            {
                sb.append(EvalUtils.unwrap(context, object));
            }
        }
        return sb.toString();
    }

    // @Override
    // public ExpressionCode generateCode(
    // CodeGeneratorContext context,
    // ExpressionCode parentCode,
    // List<Expression> arguments)
    // {
    // ExpressionCode code = ExpressionCode.code(context, parentCode);
    // context.addImport("se.kuseman.payloadbuilder.core.utils.ObjectUtils");
    //
    // List<String> argsResVars = new ArrayList<>(arguments.size());
    // StringBuilder sb = new StringBuilder();
    // for (Expression arg : arguments)
    // {
    // ExpressionCode argCode = arg.generateCode(context, parentCode);
    // argsResVars.add(argCode.getResVar());
    // sb.append(argCode.getCode());
    // }
    //
    // // TODO: Fix iterator concating even if arguments are object
    //
    // String template = "%s"
    // + "Object %s = ObjectUtils.concat(%s);\n";
    //
    // code.setCode(String.format(template,
    // sb.toString(),
    // code.getIsNull(),
    // code.getResVar(),
    // argsResVars.stream().collect(joining(","))));
    // return code;
    // }
}
