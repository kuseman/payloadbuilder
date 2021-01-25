package org.kuse.payloadbuilder.core.parser;

/** Cache flush statement */
public class CacheFlushStatement extends Statement
{
    private final String name;
    private final Expression key;
    private final boolean all;

    private CacheFlushStatement(
            String name,
            Expression key,
            boolean all)
    {
        this.name = name;
        this.all = all;
        this.key = key;
    }

    public String getName()
    {
        return name;
    }

    public Expression getKey()
    {
        return key;
    }

    public boolean isAll()
    {
        return all;
    }

    @Override
    public <TR, TC> TR accept(StatementVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    static CacheFlushStatement all()
    {
        return new CacheFlushStatement(null, null, true);
    }

    static CacheFlushStatement cache(String name, Expression key)
    {
        return new CacheFlushStatement(name, key, false);
    }
}
