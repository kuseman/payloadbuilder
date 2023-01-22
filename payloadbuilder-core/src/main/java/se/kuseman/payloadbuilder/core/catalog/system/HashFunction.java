package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.utils.VectorUtils;

/** Hash function. Hahses arguments using Objects.hash */
class HashFunction extends ScalarFunctionInfo
{
    HashFunction(Catalog catalog)
    {
        super(catalog, "hash", FunctionType.SCALAR);
    }

    @Override
    public ResolvedType getType(List<? extends IExpression> arguments)
    {
        return ResolvedType.of(Column.Type.Int);
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<? extends IExpression> arguments)
    {
        int size = arguments.size();
        final ValueVector[] vectors = new ValueVector[size];
        for (int i = 0; i < size; i++)
        {
            vectors[i] = arguments.get(i)
                    .eval(input, context);
        }

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
            public boolean isNull(int row)
            {
                for (int i = 0; i < size; i++)
                {
                    if (vectors[i].isNull(row))
                    {
                        return true;
                    }
                }

                return false;
            }

            @Override
            public int getInt(int row)
            {
                return VectorUtils.hash(vectors, row);
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
    // int size = arguments.size();
    // Object[] args = new Object[size];
    // for (int i = 0; i < size; i++)
    // {
    // args[i] = arguments.get(i)
    // .eval(context);
    // if (args[i] == null)
    // {
    // return null;
    // }
    // }
    // return Objects.hash(args);
    // }

    // @Override
    // public ExpressionCode generateCode(
    // CodeGeneratorContext context,
    // ExpressionCode parentCode,
    // List<Expression> arguments)
    // {
    // ExpressionCode code = ExpressionCode.code(context);
    //
    // StringBuilder sb = new StringBuilder();
    // List<String> argVars = new ArrayList<>();
    // List<String> isNullVars = new ArrayList<>();
    //
    // for (Expression arg : arguments)
    // {
    // ExpressionCode argCode = arg.generateCode(context, parentCode);
    // sb.append(argCode.getCode());
    // argVars.add(argCode.getResVar());
    // isNullVars.add(argCode.getIsNull());
    // }
    //
    // String template = "%s"
    // + "boolean %s = true;\n"
    // + "int %s = -1;\n"
    // + "if (!(%s))\n"
    // + "{\n"
    // + " %s = java.util.Objects.hash(%s);\n"
    // + " %s = false;\n"
    // + "}\n";
    //
    // code.setCode(String.format(template,
    // sb.toString(),
    // code.getIsNull(),
    // code.getResVar(),
    // isNullVars.stream().collect(joining("||")),
    // code.getResVar(), argVars.stream().collect(joining(",")),
    // code.getIsNull()));
    //
    // return code;
    // }
}
