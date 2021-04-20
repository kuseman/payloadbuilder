package org.kuse.payloadbuilder.core.operator;

/** Exception thrown from operators */
public class OperatorException extends RuntimeException
{
    public OperatorException(String message)
    {
        super(message);
    }

    public OperatorException(String message, Exception e)
    {
        super(message, e);
    }
}
