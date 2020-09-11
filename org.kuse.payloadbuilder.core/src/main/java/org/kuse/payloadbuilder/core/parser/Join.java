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

/** Inner, left join */
public class Join extends AJoin
{
    private final Expression condition;
    private final JoinType type;

    public Join(
            TableSource tableSource,
            JoinType type,
            Expression condition)
    {
        super(tableSource);
        this.type = requireNonNull(type, "type");
        this.condition = requireNonNull(condition, "condition");
    }

    public JoinType getType()
    {
        return type;
    }

    public Expression getCondition()
    {
        return condition;
    }

    @Override
    public <TR, TC> TR accept(SelectVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public String toString()
    {
        return type + " JOIN " + getTableSource() + "\tON " + condition;
    }

    public enum JoinType
    {
        INNER,
        LEFT;
    }
}
