package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;
import java.util.function.BiFunction;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.api.operator.Tuple;
import se.kuseman.payloadbuilder.core.operator.IGroupedRow;
import se.kuseman.payloadbuilder.core.operator.StatementContext;

abstract class AggregateFunction extends ScalarFunctionInfo
{
    AggregateFunction(Catalog catalog, String name)
    {
        super(catalog, name);
    }

    /** Return the identity value when aggregating grouped rows */
    protected Object getAggregatorIdentity()
    {
        return null;
    }

    /** Return the accumulator used when aggregating grouped rows */
    abstract BiFunction<Object, Object, Object> getAggregatorAccumulator();

    /**
     * Return the value for this aggregate function for a constant expression. IE. COUNT(1), MAX(10) etc.
     */
    abstract Object getAggregatorConstantValue(Object value, List<Tuple> tuples);

    /** Evaluate this function in a non aggregated context */
    abstract Object evalNonAggregated(IExecutionContext context, String catalogAlias, List<? extends IExpression> arguments);

    @Override
    public Object eval(IExecutionContext context, String catalogAlias, List<? extends IExpression> arguments)
    {
        StatementContext stmCtx = (StatementContext) context.getStatementContext();
        Tuple tuple = stmCtx.getTuple();

        // Aggregation of grouped rows
        if (tuple instanceof IGroupedRow)
        {
            List<Tuple> tuples = ((IGroupedRow) tuple).getContainedRows();

            IExpression exp = arguments.get(0);
            if (exp.isConstant())
            {
                return getAggregatorConstantValue(exp.eval(context), tuples);
            }

            Object result = getAggregatorIdentity();
            BiFunction<Object, Object, Object> accumulator = getAggregatorAccumulator();

            int size = tuples.size();
            for (int i = 0; i < size; i++)
            {
                stmCtx.setTuple(tuples.get(i));
                result = accumulator.apply(result, exp.eval(context));
            }
            // Restore context tuple
            stmCtx.setTuple(tuple);
            return result;
        }

        return evalNonAggregated(context, catalogAlias, arguments);
    }
}
