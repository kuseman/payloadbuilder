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
import se.kuseman.payloadbuilder.core.catalog.LambdaFunction;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.expression.LambdaExpression;

/** Map function. Maps input into another form */
class MapFunction extends ScalarFunctionInfo implements LambdaFunction
{
    private static final List<LambdaBinding> LAMBDA_BINDINGS = singletonList(new LambdaBinding(1, 0));

    MapFunction(Catalog catalog)
    {
        super(catalog, "map", FunctionType.SCALAR);
    }

    // @Override
    // public Set<TableAlias> resolveAlias(Set<TableAlias> parentAliases, List<Set<TableAlias>> argumentAliases)
    // {
    // // Map function has resulting alias as the mapping lambda
    // return argumentAliases.get(1);
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
        LambdaExpression le = (LambdaExpression) arguments.get(1);

        Type inputType = arguments.get(0)
                .getType()
                .getType();

        if (inputType == Type.ValueVector
                || inputType == Type.TupleVector)
        {
            return ResolvedType.valueVector(le.getExpression()
                    .getType());
        }

        return le.getExpression()
                .getType();
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<? extends IExpression> arguments)
    {
        ValueVector value = arguments.get(0)
                .eval(input, context);
        final LambdaExpression le = (LambdaExpression) arguments.get(1);
        final Type type = value.type()
                .getType();
        if (type == Type.ValueVector
                || type == Type.TupleVector)
        {
            return new ValueVector()
            {
                LambdaUtils.RowTupleVector inputTupleVector = new LambdaUtils.RowTupleVector(input);

                @Override
                public ResolvedType type()
                {
                    return ResolvedType.valueVector(le.getExpression()
                            .getType());
                }

                @Override
                public int size()
                {
                    return value.size();
                }

                @Override
                public boolean isNull(int row)
                {
                    // Map a null is null
                    return value.isNull(row);
                }

                @Override
                public Object getValue(int row)
                {
                    ValueVector lambdaValue;
                    if (type == Type.TupleVector)
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

                    return le.getExpression()
                            .eval(inputTupleVector, context);
                }
            };
        }

        // Rest of the primitive types, just eval the lambda with inputs
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
