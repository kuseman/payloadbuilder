package com.viskan.payloadbuilder.parser.tree;

import static java.util.Objects.requireNonNull;

/** Standard join producing tuples */
public class Join
{
    private final QualifiedName qname;
    private final String alias;
    private final Expression condition;
    private final JoinType type;

    public Join(QualifiedName qname, String alias, JoinType type, Expression condition)
    {
        this.qname = requireNonNull(qname, "qname");
        this.alias = requireNonNull(alias, "alias");
        this.type = requireNonNull(type, "type");
        this.condition = requireNonNull(condition, "condiftion");
    }

    public enum JoinType
    {
        INNER,
        LEFT;
    }
}
