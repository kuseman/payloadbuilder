package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Collections.emptyList;

import java.util.ArrayList;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.core.operator.EvalUtils;
import se.kuseman.payloadbuilder.core.operator.ExecutionContext;

/** Function listOf. Creates a list of provided arguments */
class ListOfFunction extends ScalarFunctionInfo
{
    ListOfFunction(Catalog catalog)
    {
        super(catalog, "listOf");
    }

    @Override
    public String getDescription()
    {
        return "Creates a list of provided arguments." + System.lineSeparator() + "ie. listOf(1,2, true, 'string')";
    }

    @Override
    public Object eval(IExecutionContext ctx, String catalogAlias, List<? extends IExpression> arguments)
    {
        ExecutionContext context = (ExecutionContext) ctx;
        int size = arguments.size();
        if (size <= 0)
        {
            return emptyList();
        }
        List<Object> result = new ArrayList<>(size);
        for (int i = 0; i < size; i++)
        {
            Object object = arguments.get(i)
                    .eval(context);
            result.add(EvalUtils.unwrap(context, object));
        }
        return result;
    }
}
