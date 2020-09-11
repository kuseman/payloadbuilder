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

/** DESCRIBE table */
public class DescribeTableStatement extends Statement
{
    private final String catalog;
    private final QualifiedName tableName;

    DescribeTableStatement(String catalog, QualifiedName tableName)
    {
        this.catalog = catalog;
        this.tableName = requireNonNull(tableName, "tableName");
    }

    public String getCatalog()
    {
        return catalog;
    }

    public QualifiedName getTableName()
    {
        return tableName;
    }

    @Override
    public <TR, TC> TR accept(StatementVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }
}
