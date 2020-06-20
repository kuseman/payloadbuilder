package com.viskan.payloadbuilder.catalog.builtin;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.LambdaFunction;
import com.viskan.payloadbuilder.catalog.ScalarFunctionInfo;
import com.viskan.payloadbuilder.evaluation.EvaluationContext;
import com.viskan.payloadbuilder.parser.tree.Expression;
import com.viskan.payloadbuilder.parser.tree.LambdaExpression;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang3.tuple.Pair;

/** Any function. Check if any of inputs is true */
abstract class AMatchFunction extends ScalarFunctionInfo implements LambdaFunction
{
    private final MatchType type;
    AMatchFunction(Catalog catalog, MatchType type)
    {
        super(catalog, type.name, Type.SCALAR);
        this.type = type;
    }
    
    @Override
    public List<Pair<Expression, LambdaExpression>> getLambdaBindings(List<Expression> arguments)
    {
        return singletonList(Pair.of(arguments.get(0), (LambdaExpression) arguments.get(1)));
    }

    @Override
    public Class<?> getDataType()
    {
        return Boolean.class;
    }
    
    @Override
    public List<Class<? extends Expression>> getInputTypes()
    {
        return asList(Expression.class, LambdaExpression.class);
    }
    
    @Override
    public Object eval(EvaluationContext context, List<Expression> arguments, Row row)
    {
        Object argResult = arguments.get(0).eval(context, row);
        if (argResult == null)
        {
            return null;
        }
        LambdaExpression le = (LambdaExpression) arguments.get(1);
        int lambdaId = le.getLambdaIds()[0];
        
        Iterator<Object> it = IteratorUtils.getIterator(argResult);
        while (it.hasNext())
        {
            context.setLambdaValue(lambdaId, it.next());
            Object obj = le.getExpression().eval(context, row);
            
            if (!(obj instanceof Boolean))
            {
                throw new IllegalArgumentException("Expected boolean result but got: " + obj + " from " + le.getExpression());
            }
            
            boolean result = (boolean) obj;
            if (type == MatchType.ALL && !result)
            {
                return false;
            }
            else if (type == MatchType.NONE && result)
            {
                return false;                        
            }
            else if (type == MatchType.ANY && result)
            {
                return true;
            }
        }
        
        return type.defaultResult;
    }
    
//    @Override
//    public ExpressionCode generateCode(
//            CodeGeneratorContext context,
//            ExpressionCode parentCode,
//            List<Expression> arguments)
//    {
//        ExpressionCode inputCode = arguments.get(0).generateCode(context, parentCode);
//        ExpressionCode code = ExpressionCode.code(context, inputCode);
//        code.addImport("com.viskan.payloadbuilder.utils.IteratorUtils");
//        code.addImport("java.util.Iterator");
//        code.addImport("org.apache.commons.collections.iterators.TransformIterator");
//        code.addImport("org.apache.commons.collections.Transformer");
//        
//        LambdaExpression le = (LambdaExpression) arguments.get(1);
//        
//        context.addLambdaParameters(le.getIdentifiers());
//        ExpressionCode lambdaCode = le.getExpression().generateCode(context, parentCode);
//        context.removeLambdaParameters(le.getIdentifiers());
//        
//        String template = 
//                "%s"
//              + "boolean %s = true;\n"
//              + "Iterator %s = null;\n"
//              + "if (!%s)\n"
//              + "{\n"
//              + "  %s = new TransformIterator(IteratorUtils.getIterator(%s), new Transformer()\n"
//              + "  {\n"
//              + "    public Object transform(Object object)\n"
//              + "    {\n"
//              + "      Object %s = object;\n"
//              + "      %s"
//              + "      return %s;\n"
//              + "    }\n"
//              + "  });\n"
//              + "  %s = false;\n"
//              + "}\n";
//        
//        code.setCode(String.format(template,
//                inputCode.getCode(),
//                code.getIsNull(),
//                code.getResVar(),
//                inputCode.getIsNull(),
//                code.getResVar(), inputCode.getResVar(),
//                le.getIdentifiers().get(0),
//                lambdaCode.getCode(),
//                lambdaCode.getResVar(),
//                code.getIsNull()));
//        
//        return code;
//    }
    
    enum MatchType
    {
        ALL("all", true),
        ANY("any", false),
        NONE("none", true);
        
        private final boolean defaultResult;
        private final String name;

        MatchType(String name, boolean defaultResult)
        {
            this.name = name;
            this.defaultResult = defaultResult;
        }
    }
}
