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

import org.antlr.v4.runtime.Token;

/** Show statement for querying current parameters/variables */
public class ShowStatement extends Statement
{
    private final Type type;
    private final String catalog;
    private final Token token;

    ShowStatement(Type type, String catalog, Token token)
    {
        this.type = type;
        this.catalog = catalog;
        this.token = token;
    }

    public Type getType()
    {
        return type;
    }

    public String getCatalog()
    {
        return catalog;
    }

    public Token getToken()
    {
        return token;
    }

    @Override
    public <TR, TC> TR accept(StatementVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    public enum Type
    {
        VARIABLES,
        TABLES,
        FUNCTIONS
    }
}
