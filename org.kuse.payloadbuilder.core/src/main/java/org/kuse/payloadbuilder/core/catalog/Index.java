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
package org.kuse.payloadbuilder.core.catalog;

import static java.util.Objects.requireNonNull;

import java.util.List;

import org.kuse.payloadbuilder.core.parser.QualifiedName;

/**
 * A table index. Defines columns that can be utilized in batching operators for quicker access to rows.
 **/
public class Index
{
    private final QualifiedName table;
    private final List<String> columns;
    private final int batchSize;

    public Index(QualifiedName table, List<String> columns, int batchSize)
    {
        this.table = requireNonNull(table, "table");
        this.columns = requireNonNull(columns, "columns");
        this.batchSize = batchSize;
    }

    public QualifiedName getTable()
    {
        return table;
    }

    public List<String> getColumns()
    {
        return columns;
    }

    public int getBatchSize()
    {
        return batchSize;
    }

    @Override
    public int hashCode()
    {
        return columns.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof Index)
        {
            Index that = (Index) obj;
            return columns.equals(that.columns)
                && batchSize == that.batchSize;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return table + " " + columns.toString();
    }
}
