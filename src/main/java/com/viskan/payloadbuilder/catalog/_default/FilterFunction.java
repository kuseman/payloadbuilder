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
import org.apache.commons.collections.iterators.FilterIterator;

/** Filter input argument with a lambda */
class FilterFunction extends ScalarFunctionInfo
{
    FilterFunction(Catalog catalog)
    {
        super(catalog, "filter", Type.SCALAR);
    }
    
    @Override
    public List<Class<? extends Expression>> getInputTypes()
    {
        return asList(Expression.class, LambdaExpression.class);
    }
    
    @Override
    public Object eval(EvaluationContext context, List<Expression> arguments, Row row)
    {
        Object arg0Result = arguments.get(0).eval(context, row);
        LambdaExpression le = (LambdaExpression) arguments.get(1);
        int lambdaId = le.getLambdaIds()[0];
        return new FilterIterator(IteratorUtils.getIterator(arg0Result), input ->
        {
            context.setLambdaValue(lambdaId, input);
            Boolean result = (Boolean) le.getExpression().eval(context, row);
            return result != null && result.booleanValue();
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
         * res1 = new org.apache.commons.collections.iterators.FilterIterator(it, new org.apache.commons.collections.Predicate()
         * {
         *   boolean evaluate(Object object)
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
              + "java.util.Iterator %s = new org.apache.commons.collections.iterators.FilterIterator(IteratorUtils.getIterator(%s), new org.apache.commons.collections.Predicate()\n"
              + "{\n"
              + "  public boolean evaluate(Object object)\n"
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
