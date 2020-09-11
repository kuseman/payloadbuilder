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
import static org.apache.commons.lang3.StringUtils.join;

/** Set statement */
public class SetStatement extends Statement
{
    private final String name;
    private final Expression expression;

    SetStatement(QualifiedName qname, Expression expression)
    {
        this.name = join(qname.getParts(), ".");
        this.expression = requireNonNull(expression, "expression");
    }

    public String getName()
    {
        return name;
    }

    public Expression getExpression()
    {
        return expression;
    }

    @Override
    public <TR, TC> TR accept(StatementVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }
}
