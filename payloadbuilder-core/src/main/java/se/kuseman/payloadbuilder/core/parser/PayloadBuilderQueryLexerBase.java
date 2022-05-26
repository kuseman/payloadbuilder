package se.kuseman.payloadbuilder.core.parser;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.atn.ATN;

/** Base lexer class. Keeps track of nesting level of braces etc. */
class PayloadBuilderQueryLexerBase extends Lexer
{
    /**
     * <pre>
     * Keeps track of the the current depth of nested template string backticks. E.g. after the X in:
     *
     * `${a ? `${X
     *
     * templateDepth will be 2. This variable is needed to determine if a `}` is a plain CloseBrace, or one that closes an expression inside a template string.
     * </pre>
     */
    private int templateDepth;

    PayloadBuilderQueryLexerBase(CharStream input)
    {
        super(input);
    }

    protected boolean isInTemplateString()
    {
        return this.templateDepth > 0;
    }

    protected void increaseTemplateDepth()
    {
        this.templateDepth++;
    }

    protected void decreaseTemplateDepth()
    {
        this.templateDepth--;
    }

    @Override
    public String[] getRuleNames()
    {
        return null;
    }

    @Override
    public String getGrammarFileName()
    {
        return null;
    }

    @Override
    public ATN getATN()
    {
        return null;
    }
}
