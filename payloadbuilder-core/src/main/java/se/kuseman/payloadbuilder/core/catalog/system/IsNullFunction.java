package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;
import java.util.function.IntFunction;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVectorAdapter;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Returns first item if not null else second item */
class IsNullFunction extends ScalarFunctionInfo
{
    IsNullFunction(Catalog catalog)
    {
        super(catalog, "isnull", FunctionType.SCALAR);
    }

    @Override
    public String getDescription()
    {
        return "Returns first non null value of provided arguments. " + System.lineSeparator()
               + "Ex. isnull(expression, expression)"
               + System.lineSeparator()
               + "If both arguments yield null, null is returned.";
    }

    @Override
    public int arity()
    {
        return 2;
    }

    @Override
    public ResolvedType getType(List<? extends IExpression> arguments)
    {
        ResolvedType typeA = arguments.get(0)
                .getType();
        ResolvedType typeB = arguments.get(1)
                .getType();

        return typeA.getType()
                .getPrecedence() >= typeB.getType()
                        .getPrecedence() ? typeA
                                : typeB;
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<? extends IExpression> arguments)
    {
        ResolvedType type = getType(arguments);
        return new ValueVectorAdapter(new IntFunction<ValueVector>()
        {
            ValueVector arg0;
            ValueVector arg1;
            ValueVector nullResult;

            @Override
            public ValueVector apply(int row)
            {
                if (arg0 == null)
                {
                    arg0 = arguments.get(0)
                            .eval(input, context);
                }

                if (!arg0.isNullable()
                        || !arg0.isNull(row))
                {
                    return arg0;
                }

                if (arg1 == null)
                {
                    arg1 = arguments.get(1)
                            .eval(input, context);
                }

                if (!arg1.isNullable()
                        || !arg1.isNull(row))
                {
                    return arg1;
                }

                if (nullResult == null)
                {
                    nullResult = ValueVector.literalNull(type, input.getRowCount());
                }

                return nullResult;
            }
        }, input.getRowCount(), true, type);
    }

    // @Override
    // public boolean isCodeGenSupported(List<? extends IExpression> arguments)
    // {
    // return arguments.get(0)
    // .isCodeGenSupported()
    // && arguments.get(1)
    // .isCodeGenSupported();
    // }
    //
    // @Override
    // public ExpressionCode generateCode(CodeGeneratorContext context, List<? extends IExpression> arguments)
    // {
    // ExpressionCode code = context.getExpressionCode();
    //
    // ExpressionCode arg0Code = arguments.get(0)
    // .generateCode(context);
    // ExpressionCode arg1Code = arguments.get(1)
    // .generateCode(context);
    //
    // /*
    // * Object v_0 = ... boolean n_0 = ....
    // *
    // * Object v_2 = v_0; boolean n_2 = n_0; if (n_0) { Object v_1 = ... boolean n_1 = ....
    // *
    // * v_2 = v_1; n_0 = n_1; }
    // */
    //
    // String template = "// isnull \n" + "%s" // arg0 code
    // + "boolean %s = %s;\n" // nullVar, arg0 nullVar
    // + "%s %s = %s;\n" // datatype, resVar, arg0 resVar
    // + "if (%s)\n" // arg0 nullVar
    // + "{\n"
    // + " %s" // arg1 code
    // + " %s = %s;\n" // nullVar, arg1 nullVar
    // + " %s = %s;\n" // resVar, arg1 resVar
    // + "}\n";
    // code.setCode(String.format(template, arg0Code.getCode(), code.getNullVar(), arg0Code.getNullVar(), context.getJavaTypeString(getDataType(arguments)), code.getResVar(), arg0Code.getResVar(),
    // arg0Code.getNullVar(), arg1Code.getCode(), code.getNullVar(), arg1Code.getNullVar(), code.getResVar(), arg1Code.getResVar()));
    // return code;
    // }
}
