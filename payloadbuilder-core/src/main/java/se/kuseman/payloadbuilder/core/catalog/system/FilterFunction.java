package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Collections.singletonList;

import java.util.List;
import java.util.NoSuchElementException;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVectorAdapter;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.LambdaFunction;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.expression.LambdaExpression;
import se.kuseman.payloadbuilder.core.physicalplan.PredicatedTupleVector;

/** Filter input argument with a lambda */
class FilterFunction extends ScalarFunctionInfo implements LambdaFunction
{
    private static final List<LambdaBinding> LAMBDA_BINDINGS = singletonList(new LambdaBinding(1, 0));

    FilterFunction(Catalog catalog)
    {
        super(catalog, "filter", FunctionType.SCALAR);
    }

    // @Override
    // public Set<TableAlias> resolveAlias(Set<TableAlias> parentAliases, List<Set<TableAlias>> argumentAliases)
    // {
    // // Resulting alias is the result of argument 0
    // return argumentAliases.get(0);
    // }

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
    public ResolvedType getType(List<? extends IExpression> arguments)
    {
        // Result type of filter is the same as input ie. argument 0
        return arguments.get(0)
                .getType();
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<? extends IExpression> arguments)
    {
        ValueVector value = arguments.get(0)
                .eval(input, context);

        LambdaExpression le = (LambdaExpression) arguments.get(1);

        Type type = value.type()
                .getType();

        // Filter each individual vector
        if (type == Type.ValueVector
                || type == Type.TupleVector)
        {
            return new ValueVector()
            {
                LambdaUtils.RowTupleVector inputTupleVector = new LambdaUtils.RowTupleVector(input);

                @Override
                public ResolvedType type()
                {
                    return value.type();
                }

                @Override
                public int size()
                {
                    return value.size();
                }

                @Override
                public boolean isNull(int row)
                {
                    // Filter a null is null
                    return value.isNull(row);
                }

                @Override
                public Object getValue(int row)
                {
                    inputTupleVector.setRow(row);
                    if (type == Type.ValueVector)
                    {
                        ValueVector lambdaValue = (ValueVector) value.getValue(row);
                        inputTupleVector.setRowCount(lambdaValue.size());
                        return createFilteredVector(context, inputTupleVector, le, lambdaValue);
                    }

                    TupleVector vector = (TupleVector) value.getValue(row);
                    inputTupleVector.setRowCount(vector.getRowCount());

                    ((ExecutionContext) context).getStatementContext()
                            .setLambdaValue(le.getLambdaIds()[0], ValueVector.literalObject(ResolvedType.tupleVector(vector.getSchema()), vector, 1));

                    ValueVector filter = le.getExpression()
                            .eval(inputTupleVector, context);

                    return new PredicatedTupleVector(vector, filter);

                }
            };
        }

        // Filter the whole input
        return createFilteredVector(context, input, le, value);
    }

    private ValueVector createFilteredVector(IExecutionContext context, TupleVector input, LambdaExpression lambdaExpression, ValueVector value)
    {
        // First evaluate the input value to create a filter vector
        ((ExecutionContext) context).getStatementContext()
                .setLambdaValue(lambdaExpression.getLambdaIds()[0], value);

        final ValueVector predicate = lambdaExpression.getExpression()
                .eval(input, context);

        final int predicateSize = predicate.size();
        final int resultSize = predicate.getCardinality();

        // Then return a filtered value vector
        return new ValueVectorAdapter(value)
        {
            @Override
            public int size()
            {
                return resultSize;
            }

            @Override
            protected int getRow(int row)
            {
                // Find match from input row
                int match = -1;
                for (int i = 0; i < predicateSize; i++)
                {
                    if (predicate.getPredicateBoolean(i))
                    {
                        match++;
                        if (match == row)
                        {
                            return i;
                        }
                    }
                }

                throw new NoSuchElementException();
            }

        };

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
