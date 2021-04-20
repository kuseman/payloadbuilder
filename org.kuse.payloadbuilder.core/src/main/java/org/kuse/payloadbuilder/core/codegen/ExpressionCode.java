package org.kuse.payloadbuilder.core.codegen;

/** Structure used when building code */
public class ExpressionCode
{
    private final String resVar;
    private final String nullVar;

    ExpressionCode(String resVar, String nullVar)
    {
        this.resVar = resVar;
        this.nullVar = nullVar;
    }

    private String code = "";

    public String getResVar()
    {
        return resVar;
    }

    public String getNullVar()
    {
        return nullVar;
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
