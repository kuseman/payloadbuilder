package com.viskan.payloadbuilder.catalog._default;

import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.ScalarFunctionInfo;
import com.viskan.payloadbuilder.codegen.CodeGenratorContext;
import com.viskan.payloadbuilder.codegen.ExpressionCode;
import com.viskan.payloadbuilder.parser.tree.Expression;

import static java.util.stream.Collectors.joining;

import java.util.ArrayList;
import java.util.List;

/** Hash function. Hahses arguments using Objects.hash */
class HashFunction extends ScalarFunctionInfo
{
    HashFunction(Catalog catalog)
    {
        super(catalog, "hash");
    }
    
    @Override
    public Class<?> getDataType()
    {
        return int.class;
    }

    @Override
    public ExpressionCode generateCode(
            CodeGenratorContext context,
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
        
        String template =
                "%s"
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