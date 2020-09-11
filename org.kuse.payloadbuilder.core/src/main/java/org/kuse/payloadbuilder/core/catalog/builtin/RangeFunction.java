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
package org.kuse.payloadbuilder.core.catalog.builtin;

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.stream.IntStream;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.TableFunctionInfo;
import org.kuse.payloadbuilder.core.operator.Operator.RowIterator;
import org.kuse.payloadbuilder.core.operator.Row;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Range table valued function that emits row in range */
class RangeFunction extends TableFunctionInfo
{
    private static final String[] COLUMNS = new String[] {"Value"};

    RangeFunction(Catalog catalog)
    {
        super(catalog, "range");
    }

    @Override
    public String[] getColumns()
    {
        return COLUMNS;
    }

    @Override
    public RowIterator open(ExecutionContext context, String catalogAlias, TableAlias tableAlias, List<Expression> arguments)
    {
        int from = 0;
        int to = -1;
        // to
        if (arguments.size() <= 1)
        {
            to = ((Number) requireNonNull(arguments.get(0).eval(context), "From argument to range cannot be null.")).intValue();
        }
        // from, to
        else if (arguments.size() <= 2)
        {
            from = ((Number) requireNonNull(arguments.get(0).eval(context), "From argument to range cannot be null.")).intValue();
            to = ((Number) requireNonNull(arguments.get(1).eval(context), "To argument to range cannot be null.")).intValue();
        }
        return RowIterator.wrap(IntStream.range(from, to).mapToObj(i -> Row.of(tableAlias, i, new Object[] {i})).iterator());
    }
}
