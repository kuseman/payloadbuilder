package com.viskan.payloadbuilder.parser.tree;

import static java.util.Objects.requireNonNull;

public class QualifiedReferenceSelectItem extends SelectItem
{
    private final QualifiedName qname;

    public QualifiedReferenceSelectItem(QualifiedName qname, String identifier)
    {
        super(identifier);
        this.qname = requireNonNull(qname, "qname");
    }
    
    public QualifiedName getQname()
    {
        return qname;
    }

    @Override
    public String toString()
    {
        return qname + super.toString();
    }
}
