package org.kuse.payloadbuilder.core.parser;

import java.util.Iterator;
import java.util.Objects;

import org.kuse.payloadbuilder.core.OutputWriter;
import org.kuse.payloadbuilder.core.operator.Projection;
import org.kuse.payloadbuilder.core.operator.Tuple;

/** Projection for wildcards. With or without alias */
public class AsteriskSelectItem extends SelectItem implements Projection
{
    private final String alias;

    public AsteriskSelectItem(String alias)
    {
        super(null, false);
        this.alias = alias;
    }

    public String getAlias()
    {
        return alias;
    }

    @Override
    public <TR, TC> TR accept(SelectVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public void writeValue(OutputWriter writer, ExecutionContext context)
    {
        Tuple tuple = context.getTuple();

        Iterator<QualifiedName> it = tuple.getQualifiedNames();
        while (it.hasNext())
        {
            QualifiedName qname = it.next();
            if (alias == null
                || qname.getParts().size() == 1
                || (qname.getParts().size() >= 2 && alias.equalsIgnoreCase(qname.getParts().get(0))))
            {
                String column = qname.getLast();
                writer.writeFieldName(column);
                writer.writeValue(tuple.getValue(qname, 0));
            }
        }
    }

    @Override
    public int hashCode()
    {
        return alias != null ? alias.hashCode() : 0;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof AsteriskSelectItem)
        {
            AsteriskSelectItem that = (AsteriskSelectItem) obj;
            return Objects.equals(alias, that.alias);
        }
        return false;
    }
}
