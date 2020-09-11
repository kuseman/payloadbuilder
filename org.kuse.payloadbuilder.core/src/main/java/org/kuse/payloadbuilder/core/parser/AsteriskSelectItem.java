/**
 *
 *  Copyright (c) Marcus Henriksson <kuseman80@gmail.com>
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package org.kuse.payloadbuilder.core.parser;

import java.util.List;
import java.util.Objects;

import org.kuse.payloadbuilder.core.OutputWriter;
import org.kuse.payloadbuilder.core.operator.Projection;
import org.kuse.payloadbuilder.core.operator.Row;
import org.kuse.payloadbuilder.core.operator.TableAlias;

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
            // The alias for the row itself
            if (alias.equals(row.getTableAlias().getAlias()))
            {
                writeRow(row, writer);
                return;
            }
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
        String[] columns = row.getColumns();
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
