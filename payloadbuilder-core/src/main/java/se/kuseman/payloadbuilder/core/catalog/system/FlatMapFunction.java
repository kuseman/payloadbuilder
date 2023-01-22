package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Collections.singletonList;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.utils.VectorUtils;
import se.kuseman.payloadbuilder.core.catalog.LambdaFunction;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.expression.LambdaExpression;

/** Flat map function. Flat maps input turning nested value vectors into flat vectors */
class FlatMapFunction extends ScalarFunctionInfo implements LambdaFunction
{
    private static final List<LambdaBinding> LAMBDA_BINDINGS = singletonList(new LambdaBinding(1, 0));

    FlatMapFunction(Catalog catalog)
    {
        super(catalog, "flatmap", FunctionType.SCALAR);
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
    public ResolvedType getType(List<? extends IExpression> arguments)
    {
        LambdaExpression le = (LambdaExpression) arguments.get(1);

        ResolvedType type = le.getExpression()
                .getType();

        ResolvedType inputType = arguments.get(0)
                .getType();

        if (type.getType() == Type.ValueVector
                && (inputType.getType() == Type.ValueVector
                        || inputType.getType() == Type.TupleVector))
        {
            return ResolvedType.valueVector(type.getSubType());
        }

        return le.getExpression()
                .getType();
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<? extends IExpression> arguments)
    {
        ValueVector value = arguments.get(0)
                .eval(input, context);

        LambdaExpression le = (LambdaExpression) arguments.get(1);

        ResolvedType type = le.getExpression()
                .getType();

        Type inputType = value.type()
                .getType();

        if (type.getType() == Type.ValueVector
                && (inputType == Type.ValueVector
                        || inputType == Type.TupleVector))
        {
            return new ValueVector()
            {
                LambdaUtils.RowTupleVector inputTupleVector = new LambdaUtils.RowTupleVector(input);

                @Override
                public ResolvedType type()
                {
                    return ResolvedType.valueVector(type.getSubType());
                }

                @Override
                public int size()
                {
                    return input.getRowCount();
                }

                @Override
                public boolean isNull(int row)
                {
                    // Flatmap:ing null is null
                    return value.isNull(row);
                }

                @Override
                public Object getValue(int row)
                {
                    ValueVector lambdaValue;
                    if (inputType == Type.TupleVector)
                    {
                        TupleVector vector = (TupleVector) value.getValue(row);
                        lambdaValue = ValueVector.literalObject(ResolvedType.tupleVector(vector.getSchema()), vector, 1);
                        inputTupleVector.setRowCount(vector.getRowCount());
                    }
                    else
                    {
                        lambdaValue = (ValueVector) value.getValue(row);
                        inputTupleVector.setRowCount(lambdaValue.size());
                    }

                    ((ExecutionContext) context).getStatementContext()
                            .setLambdaValue(le.getLambdaIds()[0], lambdaValue);

                    inputTupleVector.setRow(row);

                    ValueVector vector = le.getExpression()
                            .eval(inputTupleVector, context);

                    // Concat all vectors into one
                    ValueVector result = null;
                    int size = vector.size();
                    for (int i = 0; i < size; i++)
                    {
                        if (result == null)
                        {
                            result = (ValueVector) vector.getValue(i);
                        }
                        else
                        {
                            result = VectorUtils.concat(result, (ValueVector) vector.getValue(i));
                        }
                    }
                    return result;
                }
            };
        }

        // Flat map flats value vectors, other values is considered flattened already so just map them with lambda expression
        ((ExecutionContext) context).getStatementContext()
                .setLambdaValue(le.getLambdaIds()[0], value);

        return le.getExpression()
                .eval(input, context);
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
