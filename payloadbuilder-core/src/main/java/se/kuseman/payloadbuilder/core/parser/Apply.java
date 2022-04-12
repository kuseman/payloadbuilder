package se.kuseman.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

/** Apply join */
public class Apply extends AJoin
{
    private final ApplyType type;

    public Apply(TableSource tableSource, ApplyType type)
    {
        super(tableSource);
        this.type = requireNonNull(type, "type");
    }

    public ApplyType getType()
    {
        return type;
    }

    @Override
    public <TR, TC> TR accept(SelectVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    /** Apply type */
    public enum ApplyType
    {
        OUTER,
        CROSS;
    }
}
