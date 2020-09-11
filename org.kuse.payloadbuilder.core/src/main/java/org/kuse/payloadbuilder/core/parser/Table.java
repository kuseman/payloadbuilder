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

import org.antlr.v4.runtime.Token;
import org.kuse.payloadbuilder.core.operator.TableAlias;

/** A table */
public class Table extends TableSource
{
    private final String catalogAlias;

    public Table(String catalogAlias, TableAlias tableAlias/*, QualifiedName table, String alias*/, List<Option> options, Token token)
    {
        super(tableAlias, options, token);
        this.catalogAlias = catalogAlias;
    }

    @Override
    public String getCatalogAlias()
    {
        return catalogAlias;
    }

    @Override
    public QualifiedName getTable()
    {
        return tableAlias.getTable();
    }

    @Override
    public <TR, TC> TR accept(SelectVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public String toString()
    {
        return tableAlias.toString();
    }
}
