package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

import org.kuse.payloadbuilder.core.cache.CacheProvider;

/** Statement for "cache flush/remove" statements */
public class CacheFlushRemoveStatement extends Statement
{
    private final CacheProvider.Type type;
    private final QualifiedName name;
    private final boolean isAll;
    private final boolean isFlush;
    private final Expression key;

    CacheFlushRemoveStatement(CacheProvider.Type type, QualifiedName name, boolean isAll, boolean isFlush, Expression key)
    {
        this.type = requireNonNull(type, "type");
        this.name = name;
        this.isAll = isAll;
        this.isFlush = isFlush;
        this.key = key;

        if (isAll && isFlush && key != null)
        {
            throw new IllegalArgumentException("Cannot flush all with a key expression");
        }
    }

    public CacheProvider.Type getType()
    {
        return type;
    }

    public QualifiedName getName()
    {
        return name;
    }

    public boolean isAll()
    {
        return isAll;
    }

    public boolean isFlush()
    {
        return isFlush;
    }

    public Expression getKey()
    {
        return key;
    }

    @Override
    public <TR, TC> TR accept(StatementVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }
}
