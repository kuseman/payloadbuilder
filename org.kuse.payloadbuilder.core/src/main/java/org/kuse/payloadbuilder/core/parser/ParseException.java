package org.kuse.payloadbuilder.core.parser;

import org.antlr.v4.runtime.Token;

public class ParseException extends RuntimeException
{
    private final int line;
    private final int column;

    public ParseException(String message, Token token)
    {
        this(message, token.getLine(), token.getCharPositionInLine());
    }
    
    public ParseException(String message, int line, int column)
    {
        super(message);
        this.line = line;
        this.column = column;
    }
    
    public int getLine()
    {
        return line;
    }
    
    public int getColumn()
    {
        return column;
    }
}
