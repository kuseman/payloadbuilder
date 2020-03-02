package com.viskan.payloadbuilder.parser.tree;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.util.List;

public class NestedSelectItem extends SelectItem
{
    private final List<SelectItem> selectItems;
    private final QualifiedName from;
    private final Expression where;
    private final Type type;

    public NestedSelectItem(
            Type type,
            List<SelectItem> selectItems,
            QualifiedName from,
            Expression where,
            String identifier)
    {
        super(identifier);
        this.type = requireNonNull(type, "type");
        this.selectItems = requireNonNull(selectItems, "selectItems");
        this.from = from;
        this.where = where;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(type).append(" (");
        sb.append(System.lineSeparator());
        sb.append(selectItems.stream().map(s -> s.toString()).collect(joining("," + System.lineSeparator(), "", System.lineSeparator())));
        if (from != null)
        {
            sb.append("FROM ").append(from);
        }
        if (where != null)
        {
            sb.append(System.lineSeparator());
            sb.append("WHERE ").append(where);
        }
        sb.append(")");
        sb.append(super.toString());
        return sb.toString();
    }
    
    public enum Type
    {
        OBJECT,ARRAY;
    }
}
