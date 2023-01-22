package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;
import java.util.Random;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Function random int, returns a random int from provided seed. */
class RandomInt extends ScalarFunctionInfo
{
    private final Random random;

    RandomInt(Catalog catalog)
    {
        super(catalog, "randomInt", FunctionType.SCALAR);
        this.random = new Random(System.nanoTime());
    }

    @Override
    public int arity()
    {
        return 1;
    }

    @Override
    public ResolvedType getType(List<? extends IExpression> arguments)
    {
        return ResolvedType.of(Type.Int);
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<? extends IExpression> arguments)
    {
        final ValueVector bound = arguments.get(0)
                .eval(input, context);
        return new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                return ResolvedType.of(Type.Int);
            }

            @Override
            public int size()
            {
                return input.getRowCount();
            }

            @Override
            public boolean isNullable()
            {
                return bound.isNullable();
            }

            @Override
            public boolean isNull(int row)
            {
                return bound.isNull(row);
            }

            @Override
            public int getInt(int row)
            {
                // Might be weird to get different values on each invocation
                // and also for each row
                return random.nextInt(bound.getInt(row));
            }

            @Override
            public Object getValue(int row)
            {
                throw new IllegalArgumentException("getValue should not be called on typed vectors");
            }
        };
    }
    //
    // @Override
    // public Object eval(IExecutionContext context, String catalogAlias, List<? extends IExpression> arguments)
    // {
    // Object boundObj = arguments.get(0)
    // .eval(context);
    // if (boundObj == null)
    // {
    // return null;
    // }
    // int bound = ((Number) boundObj).intValue();
    // return random.nextInt(bound);
    // }

    // @Override
    // public ExpressionCode generateCode(CodeGeneratorContext context, ExpressionCode parentCode, List<Expression> arguments)
    // {
    // ExpressionCode code = ExpressionCode.code(context);
    // code.addImport("java.util.Random");
    //
    // ExpressionCode argCode = arguments.get(0).generateCode(context, parentCode);
    //
    // code.setCode(String.format(
    // "%s"
    // + "boolean %s = true;\n"
    // + "int %s = 0;\n"
    // + "if (!%s)\n"
    // + "{\n"
    // + " %s = false;\n"
    // + " %s = new java.util.Random().nextInt(((Number) %s).intValue());\n"
    // + "}\n",
    // argCode.getCode(),
    // code.getIsNull(),
    // code.getResVar(),
    // argCode.getIsNull(),
    // code.getIsNull(),
    // code.getResVar(),
    // argCode.getResVar()));
    // return code;
    // }
}
