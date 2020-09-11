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

/** Unit test of {@link ComparisonExpression} */
public class ComparisonExpressionTest extends AParserTest
{
    @Test
    public void test_fold()
    {
        Expression e;

        e = e("1=1");
        assertTrue(e.isConstant());
        assertEquals(TRUE_LITERAL, e);
        e = e("1 > 1");
        assertEquals(FALSE_LITERAL, e);
        e = e("1=a");
        assertFalse(e.isConstant());
        assertEquals(e("1=a"), e);
        e = e("a=1");
        assertEquals(e("a=1"), e);
        assertFalse(e.isConstant());

        e = e("null=1");
        assertEquals(NULL_LITERAL, e);
        e = e("1=null");
        assertEquals(NULL_LITERAL, e);
        e = e("(1+2) > 10");
        assertEquals(FALSE_LITERAL, e);
        e = e("10 > (1+2)");
        assertEquals(TRUE_LITERAL, e);
        e = e("(1+2+a) > 10");
        assertEquals(e("(3+a) > 10"), e);
    }
}
