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

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.util.List;

public class TableSourceJoined extends ASelectNode
{
    private final TableSource tableSource;
    private final List<AJoin> joins;

    public TableSourceJoined(TableSource tableSource, List<AJoin> joins)
    {
        this.tableSource = requireNonNull(tableSource, "tableSource");
        this.joins = requireNonNull(joins, "joins");
    }

    public TableSource getTableSource()
    {
        return tableSource;
    }

    public List<AJoin> getJoins()
    {
        return joins;
    }

    @Override
    public <TR, TC> TR accept(SelectVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public String toString()
    {
        return tableSource.toString() + (!joins.isEmpty() ? System.lineSeparator() + joins.stream().map(j -> j.toString()).collect(joining(System.lineSeparator())) : "");
    }
}
