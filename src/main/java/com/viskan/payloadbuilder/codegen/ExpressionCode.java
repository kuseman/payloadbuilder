package com.viskan.payloadbuilder.codegen;

/** Structure used when building code */
public class ExpressionCode
{
    private ExpressionCode()
    {}
    
    String code = "";
    String resVar;
    String isNull;

    public String getResVar()
    {
        return resVar;
    }
    
    public String getIsNull()
    {
        return isNull;
    }
    
    public String getCode()
    {
        return code;
    }

    public void setCode(String code)
    {
        this.code = code;
    }
    
    /** Create a new code from context
     * Generates a new result and isnull variable 
     **/
    public static ExpressionCode code(CodeGeneratorContext context)
    {
        ExpressionCode ec = new ExpressionCode();
        ec.resVar = context.newVar("res");
        ec.isNull = context.newVar("isNull");
        return ec;
    }
}
