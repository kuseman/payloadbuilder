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

import org.junit.Test;

/** Unit test of {@link LogicalBinaryExpression} */
public class LogicalBinaryExpressionTest extends AParserTest
{
    @Test
    public void test_fold()
    {
        Expression e;

        e = e("true AND a");
        assertFalse(e.isConstant());
        assertEquals(e("a"), e);
        e = e("false AND a");
        assertEquals(FALSE_LITERAL, e);
        e = e("null AND a");
        assertEquals(e("null AND a"), e);
        e = e("null AND false");
        assertEquals(FALSE_LITERAL, e);

        e = e("true OR a");
        assertEquals(TRUE_LITERAL, e);
        e = e("false OR a");
        assertEquals(e("a"), e);
        e = e("null OR a");
        assertEquals(e("null oR a"), e);

        e = e("a AND true");
        assertFalse(e.isConstant());
        assertEquals(e("a"), e);
        e = e("a AND false");
        assertEquals(FALSE_LITERAL, e);
        e = e("a AND null");
        assertEquals(e("a AND null"), e);

        e = e("a OR true");
        assertEquals(TRUE_LITERAL, e);
        e = e("a OR false");
        assertEquals(e("a"), e);
        e = e("a OR null");
        assertEquals(e("a OR null"), e);

        e = e("a OR b");
        assertEquals(e("a OR b"), e);

        e = e("true OR false");
        assertTrue(e.isConstant());

        e = e("(a AND b AND c AND d) OR true");
        assertEquals(TRUE_LITERAL, e);

        e = e("(a AND b AND c AND d) OR false");
        assertEquals(e("a AND b AND c AND d"), e);

        e = e("(a AND true) OR (b and c)");
        assertFalse(e.isConstant());
        assertEquals(e("a OR (b and c)"), e);
    }
}
