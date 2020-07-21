package com.viskan.payloadbuilder.parser;

import com.viskan.payloadbuilder.OutputWriter;
import com.viskan.payloadbuilder.catalog.TableAlias;
import com.viskan.payloadbuilder.operator.Projection;
import com.viskan.payloadbuilder.operator.Row;

import java.util.List;
import java.util.Objects;

/** Projection for wildcards. With or without alias */
public class AsteriskSelectItem extends SelectItem implements Projection
{
    private final String alias;
    AsteriskSelectItem(String alias)
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
        Row row = context.getRow();
        if (alias == null)
        {
            writeRow(row, writer);
            for (TableAlias childAlias : row.getTableAlias().getChildAliases())
            {
                List<Row> childRows = row.getChildRows(childAlias.getParentIndex());
                if (childRows.isEmpty())
                {
                    continue;
                }

                writeRow(childRows.get(0), writer);
            }
        }
        else
        {
            TableAlias childAlias = row.getTableAlias().getChildAlias(alias);
            if (childAlias != null)
            {
                List<Row> childRows = row.getChildRows(childAlias.getParentIndex());
                if (!childRows.isEmpty())
                {
                    writeRow(childRows.get(0), writer);
                }
            }
        }
    }

    private void writeRow(Row row, OutputWriter writer)
    {
        String[] columns = row.getTableAlias().getColumns();
        if (columns == null)
        {
            return;
        }
        for (int i = 0; i < columns.length; i++)
        {
            writer.writeFieldName(columns[i]);
            writer.writeValue(row.getObject(i));
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
