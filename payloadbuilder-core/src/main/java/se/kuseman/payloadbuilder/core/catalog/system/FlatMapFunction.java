package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Collections.emptyIterator;
import static java.util.Collections.singletonList;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.Transformer;
import org.apache.commons.collections4.iterators.ObjectGraphIterator;

import se.kuseman.payloadbuilder.api.TableAlias;
import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.core.catalog.LambdaFunction;
import se.kuseman.payloadbuilder.core.operator.StatementContext;
import se.kuseman.payloadbuilder.core.parser.LambdaExpression;
import se.kuseman.payloadbuilder.core.utils.CollectionUtils;

/** Flat map function. Flat maps input */
class FlatMapFunction extends ScalarFunctionInfo implements LambdaFunction
{
    private static final List<LambdaBinding> LAMBDA_BINDINGS = singletonList(new LambdaBinding(1, 0));

    FlatMapFunction(Catalog catalog)
    {
        super(catalog, "flatmap");
    }

    @Override
    public Set<TableAlias> resolveAlias(Set<TableAlias> parentAliases, List<Set<TableAlias>> argumentAliases)
    {
        // Result of flat map is the result of the lambda
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
        return new ObjectGraphIterator<>(CollectionUtils.getIterator(argResult),
                // CSOFF
                new Transformer<Object, Object>()
                // CSON
                {
                    private Iterator<Object> it;

                    @Override
                    public Object transform(Object input)
                    {
                        if (it == null)
                        {
                            ctx.setLambdaValue(lambdaId, input);
                            Object value = le.getExpression()
                                    .eval(context);
                            if (value == null)
                            {
                                return emptyIterator();
                            }
                            it = CollectionUtils.getIterator(value);
                            Object result = it;
                            if (!it.hasNext())
                            {
                                it = null;
                            }
                            return result;
                        }
                        else if (!it.hasNext())
                        {
                            it = null;
                        }
                        return input;
                    }
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
    // code.addImport("org.apache.commons.collections.iterators.ObjectGraphIterator");
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
    // + " %s = new ObjectGraphIterator(IteratorUtils.getIterator(%s), new Transformer()\n"
    // + " {\n"
    // + " Iterator<Object> it;\n"
    // + " public Object transform(Object input)\n"
    // + " {\n"
    // + " if (it == null)\n"
    // + " {\n"
    // + " Object %s = input;\n"
    // + " %s"
    // + " it = IteratorUtils.getIterator(%s);\n"
    // + " return it;\n"
    // + " }\n"
    // + " else if (!it.hasNext())\n"
    // + " {\n"
    // + " it=null;\n"
    // + " }\n"
    // + " return input;\n"
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
