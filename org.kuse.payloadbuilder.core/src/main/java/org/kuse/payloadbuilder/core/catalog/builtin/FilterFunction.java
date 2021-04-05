package org.kuse.payloadbuilder.core.catalog.builtin;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.iterators.FilterIterator;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.LambdaFunction;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.LambdaExpression;
import org.kuse.payloadbuilder.core.utils.CollectionUtils;

/** Filter input argument with a lambda */
class FilterFunction extends ScalarFunctionInfo implements LambdaFunction
{
    private static final List<LambdaBinding> LAMBDA_BINDINGS = singletonList(
            new LambdaBinding(1, 0));

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
    public Class<?> getDataType()
    {
        return Iterator.class;
    }

    @Override
    public List<Class<? extends Expression>> getInputTypes()
    {
        return asList(Expression.class, LambdaExpression.class);
    }

    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        Object argResult = arguments.get(0).eval(context);
        if (argResult == null)
        {
            return null;
        }
        LambdaExpression le = (LambdaExpression) arguments.get(1);
        int lambdaId = le.getLambdaIds()[0];
        return new FilterIterator(CollectionUtils.getIterator(argResult), input ->
        {
            context.setLambdaValue(lambdaId, input);
            Boolean result = (Boolean) le.getExpression().eval(context);
            return result != null && result.booleanValue();
        });
    }

    //    @Override
    //    public ExpressionCode generateCode(
    //            CodeGeneratorContext context,
    //            ExpressionCode parentCode,
    //            List<Expression> arguments)
    //    {
    //        ExpressionCode inputCode = arguments.get(0).generateCode(context, parentCode);
    //        ExpressionCode code = ExpressionCode.code(context, inputCode);
    //        code.addImport("org.kuse.payloadbuilder.core.utils.CollectionUtils");
    //        code.addImport("java.util.Iterator");
    //        code.addImport("org.apache.commons.collections.iterators.FilterIterator");
    //        code.addImport("org.apache.commons.collections.Predicate");
    //
    //        LambdaExpression le = (LambdaExpression) arguments.get(1);
    //
    //        context.addLambdaParameters(le.getIdentifiers());
    //        ExpressionCode lambdaCode = le.getExpression().generateCode(context, parentCode);
    //        context.removeLambdaParameters(le.getIdentifiers());
    //
    //        String template = "%s"
    //            + "boolean %s = true;\n"
    //            + "Iterator %s = null;\n"
    //            + "if (!%s)\n"
    //            + "{\n"
    //            + "  %s = new FilterIterator(IteratorUtils.getIterator(%s), new Predicate()\n"
    //            + "  {\n"
    //            + "    public boolean evaluate(Object object)\n"
    //            + "    {\n"
    //            + "      Object %s = object;\n"
    //            + "      %s"
    //            + "      return %s;\n"
    //            + "    }\n"
    //            + "  });\n"
    //            + "  %s = false;\n"
    //            + "}\n";
    //
    //        code.setCode(String.format(template,
    //                inputCode.getCode(),
    //                code.getIsNull(),
    //                code.getResVar(),
    //                inputCode.getIsNull(),
    //                code.getResVar(), inputCode.getResVar(),
    //                le.getIdentifiers().get(0),
    //                lambdaCode.getCode(),
    //                lambdaCode.getResVar(),
    //                code.getIsNull()));
    //
    //        return code;
    //    }
}
