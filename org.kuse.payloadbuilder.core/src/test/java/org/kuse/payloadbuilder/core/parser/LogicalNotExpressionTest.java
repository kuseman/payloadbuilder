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

import static org.kuse.payloadbuilder.core.parser.LiteralBooleanExpression.FALSE_LITERAL;
import static org.kuse.payloadbuilder.core.parser.LiteralBooleanExpression.TRUE_LITERAL;
import static org.kuse.payloadbuilder.core.parser.LiteralNullExpression.NULL_LITERAL;

import org.junit.Test;

/** Unit test of {@link LogicalNotExpression} */
public class LogicalNotExpressionTest extends AParserTest
{
    @Test
    public void test_fold()
    {
        Expression e;

        e = e("not true");
        assertTrue(e.isConstant());
        assertEquals(FALSE_LITERAL, e);
        e = e("not false");
        assertEquals(TRUE_LITERAL, e);
        e = e("not a");
        assertFalse(e.isConstant());
        assertEquals(e("not a"), e);
        e = e("not null");
        assertEquals(NULL_LITERAL, e);

        e = e("not (true AND a)");
        assertFalse(e.isConstant());
        assertEquals(e("not a"), e);
    }
}
