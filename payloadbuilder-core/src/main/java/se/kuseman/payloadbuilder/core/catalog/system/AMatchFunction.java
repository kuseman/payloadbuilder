package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Collections.singletonList;

import java.util.Iterator;
import java.util.List;

import se.kuseman.payloadbuilder.api.TableMeta;
import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.core.catalog.LambdaFunction;
import se.kuseman.payloadbuilder.core.operator.StatementContext;
import se.kuseman.payloadbuilder.core.parser.LambdaExpression;
import se.kuseman.payloadbuilder.core.utils.CollectionUtils;

/** Any function. Check if any of inputs is true */
class AMatchFunction extends ScalarFunctionInfo implements LambdaFunction
{
    private static final List<LambdaBinding> LAMBDA_BINDINGS = singletonList(new LambdaBinding(1, 0));

    private final MatchType type;

    AMatchFunction(Catalog catalog, MatchType type)
    {
        super(catalog, type.name);
        this.type = type;
    }

    @Override
    public List<LambdaBinding> getLambdaBindings()
    {
        return LAMBDA_BINDINGS;
    }

    @Override
    public TableMeta.DataType getDataType(List<? extends IExpression> arguments)
    {
        return TableMeta.DataType.BOOLEAN;
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

        Iterator<Object> it = CollectionUtils.getIterator(argResult);
        while (it.hasNext())
        {
            ctx.setLambdaValue(lambdaId, it.next());
            Object obj = le.getExpression()
                    .eval(context);

            if (!(obj instanceof Boolean))
            {
                throw new IllegalArgumentException("Expected boolean result but got: " + obj + " from " + le.getExpression());
            }

            boolean result = (boolean) obj;
            if (type == MatchType.ALL
                    && !result)
            {
                return false;
            }
            else if (type == MatchType.NONE
                    && result)
            {
                return false;
            }
            else if (type == MatchType.ANY
                    && result)
            {
                return true;
            }
        }

        return type.defaultResult;
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
    // String template =
    // "%s"
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

    /** Match type */
    enum MatchType
    {
        ALL("all", true),
        ANY("any", false),
        NONE("none", true);

        private final boolean defaultResult;
        private final String name;

        MatchType(String name, boolean defaultResult)
        {
            this.name = name;
            this.defaultResult = defaultResult;
        }
    }
}
