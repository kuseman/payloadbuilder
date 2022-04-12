package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Collections.singletonList;

import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.iterators.FilterIterator;

import se.kuseman.payloadbuilder.api.TableAlias;
import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.core.catalog.LambdaFunction;
import se.kuseman.payloadbuilder.core.operator.StatementContext;
import se.kuseman.payloadbuilder.core.parser.LambdaExpression;
import se.kuseman.payloadbuilder.core.utils.CollectionUtils;

/** Filter input argument with a lambda */
class FilterFunction extends ScalarFunctionInfo implements LambdaFunction
{
    private static final List<LambdaBinding> LAMBDA_BINDINGS = singletonList(new LambdaBinding(1, 0));

    FilterFunction(Catalog catalog)
    {
        super(catalog, "filter");
    }

    @Override
    public Set<TableAlias> resolveAlias(Set<TableAlias> parentAliases, List<Set<TableAlias>> argumentAliases)
    {
        // Resulting alias is the result of argument 0
        return argumentAliases.get(0);
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
    public Object eval(IExecutionContext context, String catalogAlias, List<? extends IExpression> arguments)
    {
        Object argResult = arguments.get(0)
                .eval(context);
        if (argResult == null)
        {
            return null;
        }
        StatementContext ctx = (StatementContext) context.getStatementContext();
        LambdaExpression le = (LambdaExpression) arguments.get(1);
        int lambdaId = le.getLambdaIds()[0];
        return new FilterIterator<>(CollectionUtils.getIterator(argResult), input ->
        {
            ctx.setLambdaValue(lambdaId, input);
            Boolean result = (Boolean) le.getExpression()
                    .eval(context);
            return result != null
                    && result.booleanValue();
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
    // code.addImport("org.apache.commons.collections.iterators.FilterIterator");
    // code.addImport("org.apache.commons.collections.Predicate");
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
    // + " %s = new FilterIterator(IteratorUtils.getIterator(%s), new Predicate()\n"
    // + " {\n"
    // + " public boolean evaluate(Object object)\n"
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
