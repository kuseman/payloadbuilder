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
package org.kuse.payloadbuilder.core.operator;

import java.util.List;

/** Merges outer and inner row */
class DefaultRowMerger implements RowMerger
{
    /** Default merger. Merges inner row into outer */
    public static final DefaultRowMerger DEFAULT = new DefaultRowMerger(-1);

    /** Limit number of merged rows */
    private final int limit;

    DefaultRowMerger(int limit)
    {
        this.limit = limit;
    }

    @Override
    public Row merge(Row outer, Row inner, boolean populating)
    {
        Row result = outer;

        // No populating join, create a copy of outer row
        if (!populating)
        {
            result = new Row(result, inner.getPos());
        }

        // Parent is always populated
        inner.addParent(result);
        List<Row> childRows = result.getChildRows(inner.getTableAlias().getParentIndex());
        if (limit < 0 || childRows.size() < limit)
        {
            childRows.add(inner);
        }
        return result;
    }

    @Override
    public int hashCode()
    {
        return limit;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof DefaultRowMerger)
        {
            return limit == ((DefaultRowMerger) obj).limit;
        }
        return false;
    }

    /** Create a limiting row merger */
    static DefaultRowMerger limit(int limit)
    {
        return new DefaultRowMerger(limit);
    }
}
