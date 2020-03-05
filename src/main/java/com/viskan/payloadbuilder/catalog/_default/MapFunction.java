package com.viskan.payloadbuilder.catalog._default;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.ScalarFunctionInfo;
import com.viskan.payloadbuilder.codegen.CodeGeneratorContext;
import com.viskan.payloadbuilder.codegen.ExpressionCode;
import com.viskan.payloadbuilder.evaluation.EvaluationContext;
import com.viskan.payloadbuilder.parser.tree.Expression;
import com.viskan.payloadbuilder.parser.tree.LambdaExpression;

import static java.util.Arrays.asList;

import java.util.List;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.collections.iterators.TransformIterator;

/** Map function. Maps input into another form */
class MapFunction extends ScalarFunctionInfo
{
    MapFunction(Catalog catalog)
    {
        super(catalog, "map", Type.SCALAR);
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
        LambdaExpression le = (LambdaExpression) arguments.get(1);
        int lambdaId = le.getLambdaIds()[0];
        return new TransformIterator(IteratorUtils.getIterator(argResult), input -> 
        {
            context.setLambdaValue(lambdaId, input);
            return le.getExpression().eval(context, row);   
        });
    }
    
    @Override
    public ExpressionCode generateCode(
            CodeGeneratorContext context,
            ExpressionCode parentCode,
            List<Expression> arguments)
    {
        ExpressionCode inputCode = arguments.get(0).generateCode(context, parentCode);
        ExpressionCode code = ExpressionCode.code(context);
        
        LambdaExpression le = (LambdaExpression) arguments.get(1);
        
        context.addLambdaParameters(le.getIdentifiers());
        ExpressionCode lambdaCode = le.getExpression().generateCode(context, parentCode);
        context.removeLambdaParameters(le.getIdentifiers());
        
        /*
         * Object res0;             // input
         * boolean isNull_0;
         * 
         * java.util.Iterator it = getIterator(res0);
         * res1 = new org.apache.commons.collections.iterators.TransformIterator(it, new org.apache.commons.collections.Transfomer()
         * {
         *   Object transform(Object object)
         *   {
         *     %lambda_code%
         *   }
         * });
         * 
         */

        // TODO: isnull input, output
        
        String template = 
                "%s"
              + "boolean %s = false;\n"
              + "java.util.Iterator %s = new org.apache.commons.collections.iterators.TransformIterator(IteratorUtils.getIterator(%s), new org.apache.commons.collections.Transformer()\n"
              + "{\n"
              + "  public Object transform(Object object)\n"
              + "  {\n"
              + "    Object %s = object;\n"
              + "    %s"
              + "    return %s;\n"
              + "  }\n"
              + "});\n";
        
        code.setCode(String.format(template,
                inputCode.getCode(),
                code.getIsNull(),
                code.getResVar(), inputCode.getResVar(),
                le.getIdentifiers().get(0),
                lambdaCode.getCode(),
                lambdaCode.getResVar()));
        
        return code;    
    }
}
