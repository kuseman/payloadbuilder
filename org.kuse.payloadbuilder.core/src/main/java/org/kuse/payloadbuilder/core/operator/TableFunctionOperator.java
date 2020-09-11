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

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.kuse.payloadbuilder.core.DescribeUtils.CATALOG;
import static org.kuse.payloadbuilder.core.utils.MapUtils.entry;
import static org.kuse.payloadbuilder.core.utils.MapUtils.ofEntries;

import java.util.List;
import java.util.Map;

import org.kuse.payloadbuilder.core.catalog.TableFunctionInfo;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Operator handling TVF's */
class TableFunctionOperator extends AOperator
{
    private final String catalogAlias;
    private final TableAlias tableAlias;
    private final TableFunctionInfo functionInfo;
    private final List<Expression> arguments;

    TableFunctionOperator(
            int nodeId,
            String catalogAlias,
            TableAlias tableAlias,
            TableFunctionInfo functionInfo,
            List<Expression> arguments)
    {
        super(nodeId);
        this.catalogAlias = catalogAlias;
        this.tableAlias = requireNonNull(tableAlias, "tableAlias");
        this.functionInfo = requireNonNull(functionInfo, "tableFunction");
        this.arguments = requireNonNull(arguments, "arguments");
    }

    @Override
    public String getName()
    {
        return "Function: " + functionInfo.getName();
    }

    @Override
    public Map<String, Object> getDescribeProperties()
    {
        return ofEntries(true,
                entry(CATALOG, functionInfo.getCatalog().getName()),
                entry("Arguments", arguments.stream().map(Object::toString).collect(toList())));
    }

    @Override
    public RowIterator open(ExecutionContext context)
    {
        return functionInfo.open(context, catalogAlias, tableAlias, arguments);
    }

    @Override
    public int hashCode()
    {
        return 17 +
            (37 * functionInfo.hashCode()) +
            (37 * arguments.hashCode());
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof TableFunctionOperator)
        {
            TableFunctionOperator that = (TableFunctionOperator) obj;
            return nodeId == that.nodeId
                && tableAlias.isEqual(that.tableAlias)
                && functionInfo.equals(that.functionInfo)
                && arguments.equals(that.arguments);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return String.format("%s (ID: %d, %s)", functionInfo.getName(), nodeId, arguments.stream().map(Object::toString).collect(joining(", ")));
    }
}
