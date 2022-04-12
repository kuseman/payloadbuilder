package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Collections.singletonList;

import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.iterators.TransformIterator;

import se.kuseman.payloadbuilder.api.TableAlias;
import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.core.catalog.LambdaFunction;
import se.kuseman.payloadbuilder.core.operator.EvalUtils;
import se.kuseman.payloadbuilder.core.operator.ExecutionContext;
import se.kuseman.payloadbuilder.core.parser.LambdaExpression;
import se.kuseman.payloadbuilder.core.utils.CollectionUtils;

/** Map function. Maps input into another form */
class MapFunction extends ScalarFunctionInfo implements LambdaFunction
{
    private static final List<LambdaBinding> LAMBDA_BINDINGS = singletonList(new LambdaBinding(1, 0));

    MapFunction(Catalog catalog)
    {
        super(catalog, "map");
    }

    @Override
    public Set<TableAlias> resolveAlias(Set<TableAlias> parentAliases, List<Set<TableAlias>> argumentAliases)
    {
        // Map function has resulting alias as the mapping lambda
        return argumentAliases.get(1);
    }

    @Override
    public List<LambdaBinding> getLambdaBindings()
    {
        return LAMBDA_BINDINGS;
    }

    @Override
    public int arity()
    {
        return 2;
    }

    @Override
    public Object eval(IExecutionContext ctx, String catalogAlias, List<? extends IExpression> arguments)
    {
        Object argResult = arguments.get(0)
                .eval(ctx);
        if (argResult == null)
        {
            return null;
        }
        ExecutionContext context = (ExecutionContext) ctx;
        LambdaExpression le = (LambdaExpression) arguments.get(1);
        int lambdaId = le.getLambdaIds()[0];
        return new TransformIterator<>(CollectionUtils.getIterator(argResult), input ->
        {
            context.getStatementContext()
                    .setLambdaValue(lambdaId, input);
            return EvalUtils.unwrap(context, le.getExpression()
                    .eval(context));
        });
    }

    // @Override
    // public ExpressionCode generateCode(
    // CodeGeneratorContext context,
    // ExpressionCode parentCode,
    // List<Expression> arguments)
    // {
    // ExpressionCode inputCode = arguments.get(0).generateCode(context, parentCode);
    // ExpressionCode code = ExpressionCode.code(context, inputCode);
    // code.addImport("se.kuseman.payloadbuilder.core.utils.CollectionUtils");
    // code.addImport("java.util.Iterator");
    // code.addImport("org.apache.commons.collections.iterators.TransformIterator");
    // code.addImport("org.apache.commons.collections.Transformer");
    //
    // LambdaExpression le = (LambdaExpression) arguments.get(1);
    //
    // context.addLambdaParameters(le.getIdentifiers());
    // ExpressionCode lambdaCode = le.getExpression().generateCode(context, parentCode);
    // context.removeLambdaParameters(le.getIdentifiers());
    //
    // String template = "%s"
    // + "boolean %s = true;\n"
    // + "Iterator %s = null;\n"
    // + "if (!%s)\n"
    // + "{\n"
    // + " %s = new TransformIterator(IteratorUtils.getIterator(%s), new Transformer()\n"
    // + " {\n"
    // + " public Object transform(Object object)\n"
    // + " {\n"
    // + " Object %s = object;\n"
    // + " %s"
    // + " return %s;\n"
    // + " }\n"
    // + " });\n"
    // + " %s = false;\n"
    // + "}\n";
    //
    // code.setCode(String.format(template,
    // inputCode.getCode(),
    // code.getIsNull(),
    // code.getResVar(),
    // inputCode.getIsNull(),
    // code.getResVar(), inputCode.getResVar(),
    // le.getIdentifiers().get(0),
    // lambdaCode.getCode(),
    // lambdaCode.getResVar(),
    // code.getIsNull()));
    //
    // return code;
    // }
}
