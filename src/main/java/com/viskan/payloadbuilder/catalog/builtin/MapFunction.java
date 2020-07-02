package com.viskan.payloadbuilder.catalog.builtin;

import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.LambdaFunction;
import com.viskan.payloadbuilder.catalog.ScalarFunctionInfo;
import com.viskan.payloadbuilder.catalog.TableAlias;
import com.viskan.payloadbuilder.codegen.CodeGeneratorContext;
import com.viskan.payloadbuilder.codegen.ExpressionCode;
import com.viskan.payloadbuilder.parser.ExecutionContext;
import com.viskan.payloadbuilder.parser.Expression;
import com.viskan.payloadbuilder.parser.LambdaExpression;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.collections.iterators.TransformIterator;
import org.apache.commons.lang3.tuple.Pair;

/** Map function. Maps input into another form */
class MapFunction extends ScalarFunctionInfo implements LambdaFunction
{
    MapFunction(Catalog catalog)
    {
        super(catalog, "map", Type.SCALAR);
    }
    
    @Override
    public Set<TableAlias> resolveAlias(Set<TableAlias> parentAliases, List<Expression> arguments, Function<Expression, Set<TableAlias>> aliasResolver)
    {
        // Map function has resulting alias as the mapping lambda
        return aliasResolver.apply(arguments.get(1));
    }
    
    @Override
    public List<Pair<Expression, LambdaExpression>> getLambdaBindings(List<Expression> arguments)
    {
        return singletonList(Pair.of(arguments.get(0), (LambdaExpression) arguments.get(1)));
    }

    @Override
    public Class<?> getDataType()
    {
        return Iterator.class;
    }
    
    @Override
    public List<Class<? extends Expression>> getInputTypes()
    {
        return asList(Expression.class, LambdaExpression.class);
    }
    
    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        Object argResult = arguments.get(0).eval(context);
        if (argResult == null)
        {
            return null;
        }
        LambdaExpression le = (LambdaExpression) arguments.get(1);
        int lambdaId = le.getLambdaIds()[0];
        return new TransformIterator(IteratorUtils.getIterator(argResult), input -> 
        {
            context.setLambdaValue(lambdaId, input);
            return le.getExpression().eval(context);   
        });
    }
    
    @Override
    public ExpressionCode generateCode(
            CodeGeneratorContext context,
            ExpressionCode parentCode,
            List<Expression> arguments)
    {
        ExpressionCode inputCode = arguments.get(0).generateCode(context, parentCode);
        ExpressionCode code = ExpressionCode.code(context, inputCode);
        code.addImport("com.viskan.payloadbuilder.utils.IteratorUtils");
        code.addImport("java.util.Iterator");
        code.addImport("org.apache.commons.collections.iterators.TransformIterator");
        code.addImport("org.apache.commons.collections.Transformer");
        
        LambdaExpression le = (LambdaExpression) arguments.get(1);
        
        context.addLambdaParameters(le.getIdentifiers());
        ExpressionCode lambdaCode = le.getExpression().generateCode(context, parentCode);
        context.removeLambdaParameters(le.getIdentifiers());
        
        String template = 
                "%s"
              + "boolean %s = true;\n"
              + "Iterator %s = null;\n"
              + "if (!%s)\n"
              + "{\n"
              + "  %s = new TransformIterator(IteratorUtils.getIterator(%s), new Transformer()\n"
              + "  {\n"
              + "    public Object transform(Object object)\n"
              + "    {\n"
              + "      Object %s = object;\n"
              + "      %s"
              + "      return %s;\n"
              + "    }\n"
              + "  });\n"
              + "  %s = false;\n"
              + "}\n";
        
        code.setCode(String.format(template,
                inputCode.getCode(),
                code.getIsNull(),
                code.getResVar(),
                inputCode.getIsNull(),
                code.getResVar(), inputCode.getResVar(),
                le.getIdentifiers().get(0),
                lambdaCode.getCode(),
                lambdaCode.getResVar(),
                code.getIsNull()));
        
        return code;
    }
}
