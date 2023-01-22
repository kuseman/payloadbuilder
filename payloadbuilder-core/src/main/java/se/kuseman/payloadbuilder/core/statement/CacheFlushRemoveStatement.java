package se.kuseman.payloadbuilder.core.statement;

import static java.util.Objects.requireNonNull;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.cache.CacheType;

/** Statement for "cache flush/remove" statements */
public class CacheFlushRemoveStatement extends Statement
{
    private final CacheType type;
    private final QualifiedName name;
    private final boolean isAll;
    private final boolean isFlush;
    private final IExpression key;

    public CacheFlushRemoveStatement(CacheType type, QualifiedName name, boolean isAll, boolean isFlush, IExpression key)
    {
        this.type = requireNonNull(type, "type");
        this.name = name;
        this.isAll = isAll;
        this.isFlush = isFlush;
        this.key = key;

        if (isAll
                && isFlush
                && key != null)
        {
            throw new IllegalArgumentException("Cannot flush all with a key expression");
        }
    }

    public CacheType getType()
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

    public IExpression getKey()
    {
        return key;
    }

    @Override
    public <TR, TC> TR accept(StatementVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }
}
