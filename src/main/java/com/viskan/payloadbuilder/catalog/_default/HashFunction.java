package com.viskan.payloadbuilder.catalog._default;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.ScalarFunctionInfo;
import com.viskan.payloadbuilder.codegen.CodeGeneratorContext;
import com.viskan.payloadbuilder.codegen.ExpressionCode;
import com.viskan.payloadbuilder.evaluation.EvaluationContext;
import com.viskan.payloadbuilder.parser.tree.Expression;

import static java.util.stream.Collectors.joining;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Hash function. Hahses arguments using Objects.hash */
class HashFunction extends ScalarFunctionInfo
{
    HashFunction(Catalog catalog)
    {
        super(catalog, "hash", Type.SCALAR);
    }
    
    @Override
    public Class<?> getDataType()
    {
        return int.class;
    }

    @Override
    public Object eval(EvaluationContext context, List<Expression> arguments, Row row)
    {
        int size = arguments.size();
        Object[] args = new Object[size];
        for (int i = 0; i < size; i++)
        {
            args[i] = arguments.get(i).eval(context, row);
            if (args[i] == null)
            {
                return null;
            }
        }
        return Objects.hash(args);
    }

    @Override
    public ExpressionCode generateCode(
            CodeGeneratorContext context,
            ExpressionCode parentCode,
            List<Expression> arguments)
    {
        ExpressionCode code = ExpressionCode.code(context);

        StringBuilder sb = new StringBuilder();
        List<String> argVars = new ArrayList<>();
        List<String> isNullVars = new ArrayList<>();

        for (Expression arg : arguments)
        {
            ExpressionCode argCode = arg.generateCode(context, parentCode);
            sb.append(argCode.getCode());
            argVars.add(argCode.getResVar());
            isNullVars.add(argCode.getIsNull());
        }

        String template = "%s"
            + "boolean %s = true;\n"
            + "int %s = -1;\n"
            + "if (!(%s))\n"
            + "{\n"
            + "  %s = java.util.Objects.hash(%s);\n"
            + "  %s = false;\n"
            + "}\n";

        code.setCode(String.format(template,
                sb.toString(),
                code.getIsNull(),
                code.getResVar(),
                isNullVars.stream().collect(joining("||")),
                code.getResVar(), argVars.stream().collect(joining(",")),
                code.getIsNull()));

        return code;
    }
}
