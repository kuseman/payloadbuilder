package org.kuse.payloadbuilder.core.codegen;

/** Structure used when building code */
public class ExpressionCode
{
    private final String resVar;

    ExpressionCode(String resVar)
    {
        this.resVar = resVar;
    }

    private String code = "";

    public String getResVar()
    {
        return resVar;
    }

    public String getCode()
    {
        return code;
    }

    public void setCode(String code)
    {
        this.code = code;
    }
}
