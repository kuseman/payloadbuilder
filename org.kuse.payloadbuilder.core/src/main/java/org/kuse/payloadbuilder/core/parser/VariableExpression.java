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

import static org.apache.commons.lang3.StringUtils.join;

/** A variable (@var) */
public class VariableExpression extends Expression
{
    private final String name;

    public VariableExpression(QualifiedName qname)
    {
        this.name = join(qname.getParts(), ".");
    }

    public String getName()
    {
        return name;
    }

    @Override
    public <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public boolean isNullable()
    {
        return true;
    }

    @Override
    public Object eval(ExecutionContext context)
    {
        return context.getVariableValue(name);
    }

    @Override
    public int hashCode()
    {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof VariableExpression)
        {
            VariableExpression that = (VariableExpression) obj;
            return name.equals(that.name);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "@" + name;
    }
}
