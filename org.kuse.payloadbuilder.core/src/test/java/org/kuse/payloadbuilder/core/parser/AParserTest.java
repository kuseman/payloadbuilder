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

import org.junit.Assert;
import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.catalog.CatalogRegistry;

/** Base class for parser tests */
abstract class AParserTest extends Assert
{
    private final QueryParser p = new QueryParser();
    protected final ExecutionContext context = new ExecutionContext(new QuerySession(new CatalogRegistry()));

    protected QueryStatement q(String query)
    {
        return p.parseQuery(query);
    }

    protected Expression e(String expression)
    {
        return p.parseExpression(expression);
    }
}
