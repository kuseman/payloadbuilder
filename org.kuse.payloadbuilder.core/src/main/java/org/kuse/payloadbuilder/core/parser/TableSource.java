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

import java.util.List;

import org.antlr.v4.runtime.Token;
import org.apache.commons.lang3.NotImplementedException;
import org.kuse.payloadbuilder.core.operator.TableAlias;

/** Base class for table sources */
public abstract class TableSource extends ASelectNode
{
    protected final TableAlias tableAlias;
    protected final Token token;
    protected final List<Option> options;

    public TableSource(TableAlias tableAlias, List<Option> options, Token token)
    {
        this.tableAlias = requireNonNull(tableAlias, "tableAlias");
        this.options = requireNonNull(options, "options");
        this.token = requireNonNull(token, "token");
    }

    public TableAlias getTableAlias()
    {
        return tableAlias;
    }

    /** Get table options (if applicable) */
    public List<Option> getOptions()
    {
        return options;
    }

    /** Return catalog (if applicable) for this table source */
    public String getCatalogAlias()
    {
        return null;
    }

    /** Return table (if applicable) for this table source */
    public QualifiedName getTable()
    {
        return null;
    }

    public Token getToken()
    {
        return token;
    }

    @Override
    public String toString()
    {
        throw new NotImplementedException(getClass().getSimpleName().toString());
    }

}
