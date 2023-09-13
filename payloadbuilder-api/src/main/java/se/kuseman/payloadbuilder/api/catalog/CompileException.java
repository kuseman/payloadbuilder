package se.kuseman.payloadbuilder.api.catalog;

/** Exception thrown during compilation of a query */
public class CompileException extends RuntimeException
{
    public CompileException(String message)
    {
        super(message);
    }
}
