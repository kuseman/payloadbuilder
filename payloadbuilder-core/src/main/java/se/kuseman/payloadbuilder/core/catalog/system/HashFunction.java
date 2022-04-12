package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;
import java.util.Objects;

import se.kuseman.payloadbuilder.api.TableMeta.DataType;
import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;

/** Hash function. Hahses arguments using Objects.hash */
class HashFunction extends ScalarFunctionInfo
{
    HashFunction(Catalog catalog)
    {
        super(catalog, "hash");
    }

    @Override
    public DataType getDataType(List<? extends IExpression> arguments)
    {
        return DataType.INT;
    }

    @Override
    public Object eval(IExecutionContext context, String catalogAlias, List<? extends IExpression> arguments)
    {
        int size = arguments.size();
        Object[] args = new Object[size];
        for (int i = 0; i < size; i++)
        {
            args[i] = arguments.get(i)
                    .eval(context);
            if (args[i] == null)
            {
                return null;
            }
        }
        return Objects.hash(args);
    }

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
