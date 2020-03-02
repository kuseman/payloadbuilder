package com.viskan.payloadbuilder.catalog._default;

import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.FunctionInfo;
import com.viskan.payloadbuilder.codegen.CodeGenratorContext;
import com.viskan.payloadbuilder.codegen.ExpressionCode;
import com.viskan.payloadbuilder.parser.tree.Expression;
import com.viskan.payloadbuilder.parser.tree.LambdaExpression;

import java.util.List;

/** Map function. Maps input into another form */
class MapFunction extends FunctionInfo
{
    MapFunction(Catalog catalog)
    {
        super(catalog, "map");
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
              + "java.util.Iterator %s = new org.apache.commons.collections.iterators.TransformIterator(getIterator(%s), new org.apache.commons.collections.Transformer()\n"
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
