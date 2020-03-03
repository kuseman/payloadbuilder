package com.viskan.payloadbuilder.catalog._default;

import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.ScalarFunctionInfo;
import com.viskan.payloadbuilder.codegen.CodeGenratorContext;
import com.viskan.payloadbuilder.codegen.ExpressionCode;
import com.viskan.payloadbuilder.parser.tree.Expression;
import com.viskan.payloadbuilder.parser.tree.LambdaExpression;

import java.util.List;

/** Filter input argument with a lambda */
class FilterFunction extends ScalarFunctionInfo
{
    FilterFunction(Catalog catalog)
    {
        super(catalog, "filter");
    }
    
    @Override
    public ExpressionCode generateCode(
            CodeGenratorContext context,
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
              + "java.util.Iterator %s = new org.apache.commons.collections.iterators.FilterIterator(getIterator(%s), new org.apache.commons.collections.Predicate()\n"
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
