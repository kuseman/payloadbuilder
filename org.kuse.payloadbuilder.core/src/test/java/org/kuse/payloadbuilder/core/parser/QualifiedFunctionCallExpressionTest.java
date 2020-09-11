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

import org.junit.Test;

/** Unit test of {@link QualifiedFunctionCallExpression} */
public class QualifiedFunctionCallExpressionTest extends AParserTest
{
    @Test
    public void test_fold()
    {
        Expression e;

        e = e("concat(1, 10)");
        assertEquals(e("concat(1,10)"), e);

        e = e("concat(1 + 1 + a, 10)");
        assertEquals(e("concat(2 + a, 10)"), e);

        e = e("a.filter(x -> 1+1+1+x > 10)");
        assertEquals(e("a.filter(x -> 3+x > 10)"), e);

        e = e("a.filter(x -> 1+1+1 > 10)");
        assertEquals(e("a.filter(x -> false)"), e);
    }
}
