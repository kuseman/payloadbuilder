package com.viskan.payloadbuilder.catalog._default;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.ScalarFunctionInfo;
import com.viskan.payloadbuilder.codegen.CodeGeneratorContext;
import com.viskan.payloadbuilder.codegen.ExpressionCode;
import com.viskan.payloadbuilder.evaluation.EvaluationContext;
import com.viskan.payloadbuilder.parser.tree.Expression;

import java.util.List;
import java.util.Random;

/** Function random int, returns a random int from provided seed. */
class RandomInt extends ScalarFunctionInfo
{
    private final Random random;

    RandomInt(Catalog catalog)
    {
        super(catalog, "randomInt", Type.SCALAR);
        this.random = new Random();
    }
    
    @Override
    public Object eval(EvaluationContext context, List<Expression> arguments, Row row)
    {
        Object boundObj = arguments.get(0).eval(context, row);
        if (boundObj == null)
        {
            return null;
        }
        int bound = ((Number) boundObj).intValue();
        return random.nextInt(bound);
    }
    
    @Override
    public ExpressionCode generateCode(CodeGeneratorContext context, ExpressionCode parentCode, List<Expression> arguments)
    {
        ExpressionCode code = ExpressionCode.code(context);
        code.addImport("java.util.Random");
        
        ExpressionCode argCode = arguments.get(0).generateCode(context, parentCode);
        
        code.setCode(String.format(
              "%s"
            + "boolean %s = true;\n"
            + "int %s = 0;\n"
            + "if (!%s)\n"
            + "{\n"
            + "  %s = false;\n"
            + "  %s = new java.util.Random().nextInt(((Number) %s).intValue());\n"
            + "}\n",
            argCode.getCode(),
            code.getIsNull(),
            code.getResVar(),
            argCode.getIsNull(),
            code.getIsNull(),
            code.getResVar(),
            argCode.getResVar()));
        return code;
    }
}
