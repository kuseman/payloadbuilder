package org.kuse.payloadbuilder.core.catalog.system;

import static java.util.Arrays.asList;

import java.util.List;
import java.util.Random;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Function random int, returns a random int from provided seed. */
class RandomInt extends ScalarFunctionInfo
{
    private final Random random;

    RandomInt(Catalog catalog)
    {
        super(catalog, "randomInt");
        this.random = new Random();
    }

    @Override
    public List<Class<? extends Expression>> getInputTypes()
    {
        return asList(Expression.class);
    }

    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        Object boundObj = arguments.get(0).eval(context);
        if (boundObj == null)
        {
            return null;
        }
        int bound = ((Number) boundObj).intValue();
        return random.nextInt(bound);
    }

    //    @Override
    //    public ExpressionCode generateCode(CodeGeneratorContext context, ExpressionCode parentCode, List<Expression> arguments)
    //    {
    //        ExpressionCode code = ExpressionCode.code(context);
    //        code.addImport("java.util.Random");
    //
    //        ExpressionCode argCode = arguments.get(0).generateCode(context, parentCode);
    //
    //        code.setCode(String.format(
    //                "%s"
    //                    + "boolean %s = true;\n"
    //                    + "int %s = 0;\n"
    //                    + "if (!%s)\n"
    //                    + "{\n"
    //                    + "  %s = false;\n"
    //                    + "  %s = new java.util.Random().nextInt(((Number) %s).intValue());\n"
    //                    + "}\n",
    //                argCode.getCode(),
    //                code.getIsNull(),
    //                code.getResVar(),
    //                argCode.getIsNull(),
    //                code.getIsNull(),
    //                code.getResVar(),
    //                argCode.getResVar()));
    //        return code;
    //    }
}
