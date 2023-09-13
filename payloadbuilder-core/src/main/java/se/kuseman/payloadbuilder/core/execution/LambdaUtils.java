package se.kuseman.payloadbuilder.core.execution;

import java.util.EnumSet;
import java.util.Set;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ObjectVector;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.core.expression.LambdaExpression;

/** Utils for lambda functions */
public class LambdaUtils
{
    private static Set<Column.Type> LAMBDA_INPUT_TYPES = EnumSet.of(Column.Type.Any, Column.Type.Array, Column.Type.Table);

    /** Returns true if provided type is supported by {@link LambdaUtils#forEachLambdaResult(IExecutionContext, TupleVector, ValueVector, LambdaExpression, LambdaResultConsumer) } */
    public static boolean supportsForEachLambdaResult(Type type)
    {
        return LAMBDA_INPUT_TYPES.contains(type);
    }

    /**
     * Evaluates a lambda expression against each Array/Table input
     *
     * @param context Execution context
     * @param input The input tuple vector provided to the lambda function
     * @param value Result of the lambda functions target expression (the result we want to apply the lambda expression on)
     * @param le The lambda expression
     * @param consumer Consumer of the lambda result
     */
    public static void forEachLambdaResult(IExecutionContext context, TupleVector input, ValueVector value, LambdaExpression le, LambdaResultConsumer consumer)
    {
        Column.Type type = value.type()
                .getType();
        if (!LAMBDA_INPUT_TYPES.contains(type))
        {
            throw new IllegalArgumentException("Input vector type must be one of: " + LAMBDA_INPUT_TYPES);
        }
        else if (le.getLambdaIds().length != 1)
        {
            throw new IllegalArgumentException("Expected a lambda expression with one parameter byt got: " + le.getLambdaIds().length);
        }

        int rowCount = value.size();
        for (int i = 0; i < rowCount; i++)
        {
            // Input types Array/Table and null, then we cannot evaluate anything
            // so "return" a null result here
            if (value.isNull(i)
                    && type != Type.Any)
            {
                consumer.accept(null, null, true);
                continue;
            }

            Object anyValue = null;
            ValueVector array = null;
            TupleVector table = null;

            // Runtime check if have a array/table
            if (type == Type.Any)
            {
                anyValue = value.valueAsObject(i);
                if (anyValue != null)
                {
                    // Is this a wrapable Array ?
                    anyValue = VectorUtils.convertToValueVector(anyValue, false);
                }
                if (anyValue instanceof ValueVector)
                {
                    array = (ValueVector) anyValue;
                }
                else if (anyValue instanceof TupleVector)
                {
                    table = (TupleVector) anyValue;
                }
            }
            else if (type == Type.Array)
            {
                array = value.getArray(i);
            }
            else if (type == Type.Table)
            {
                table = value.getTable(i);
            }

            Object inputResult = null;
            boolean inputWasListType = true;
            LambdaUtils.RowTupleVector inputTupleVector = null;
            if (array != null)
            {
                inputResult = array;
                inputTupleVector = new RowTupleVector(input, i, array.size());
            }
            else if (table != null)
            {
                inputResult = table;
                inputTupleVector = new RowTupleVector(input, i, table.getRowCount());

                // Split all table rows into ObjectVectors
                final TupleVector tv = table;
                array = new ValueVector()
                {
                    @Override
                    public ResolvedType type()
                    {
                        return ResolvedType.object(tv.getSchema());
                    }

                    @Override
                    public int size()
                    {
                        return tv.getRowCount();
                    }

                    @Override
                    public boolean isNull(int row)
                    {
                        return false;
                    }

                    @Override
                    public ObjectVector getObject(int row)
                    {
                        return ObjectVector.wrap(tv, row);
                    }
                };
            }
            else
            {
                inputResult = anyValue;
                inputWasListType = false;
                inputTupleVector = new RowTupleVector(input, i, 1);
                array = anyValue == null ? ValueVector.literalNull(ResolvedType.of(Type.Any), 1)
                        : ValueVector.literalAny(1, anyValue);
            }

            ExecutionContext ctx = ((ExecutionContext) context);

            ctx.getStatementContext()
                    .setLambdaValue(le.getLambdaIds()[0], array);

            consumer.accept(inputResult, le.getExpression()
                    .eval(inputTupleVector, ctx), inputWasListType);
        }
    }

    /** Consumer used in result in {@link LambdaUtils#forEachLambdaResult} */
    @FunctionalInterface
    public interface LambdaResultConsumer
    {
        /**
         * Accept lambda result for each row in input
         *
         * @param inputResult The resulting value used as input to lambda expression. Can be either ValueVector/TupleVector or any object
         * @param lambdaResult Resulting value for each lambda evaluation. Is null if input row was null
         * @param inputWasListType True if input result was of Array/Table type. NOTE! Can only be false when input type is Any and value was not of Array/Table type
         */
        void accept(Object inputResult, ValueVector lambdaResult, boolean inputWasListType);
    }

    /** Value vector wrapper that acts as a single row tuple vector. Used in each iteration for lambdas */
    public static class RowTupleVector implements TupleVector
    {
        private final TupleVector wrapped;
        private final int row;
        private final int rowCount;

        public RowTupleVector(TupleVector vector, int row, int rowCount)
        {
            this.wrapped = vector;
            this.row = row;
            this.rowCount = rowCount;
        }

        @Override
        public int getRowCount()
        {
            return rowCount;
        }

        @Override
        public ValueVector getColumn(int column)
        {
            final ValueVector vector = wrapped.getColumn(column);
            return new ValueVectorAdapter(vector)
            {
                @Override
                public int size()
                {
                    return rowCount;
                }

                @Override
                protected int getRow(int row)
                {
                    return RowTupleVector.this.row;
                }
            };
        }

        @Override
        public Schema getSchema()
        {
            return wrapped.getSchema();
        }
    }
}
