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
package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;

import org.junit.Test;
import org.kuse.payloadbuilder.core.parser.QualifiedName;

/** Test of {@link OperatorBuilder} building applys */
public class OperatorBuilderApplyTest extends AOperatorTest
{
    private final TableAlias source = TableAlias.of(null, QualifiedName.of("source"), "s", new String[] {"id1"});
    @SuppressWarnings("unused")
    private final TableAlias article = TableAlias.of(source, QualifiedName.of("article"), "a", new String[] {"id2"});

    @Test
    public void test_cross_apply()
    {
        String query = "select s.id1, a.id2 from source s cross apply article a";
        QueryResult result = getQueryResult(query);

        assertTrue("Alias hierarchy should be equal", source.isEqual(result.alias));

        Operator expected = new NestedLoopJoin(
                3,
                "",
                result.tableOperators.get(0),
                new CachingOperator(2, result.tableOperators.get(1)),
                null,
                DefaultRowMerger.DEFAULT,
                false,
                false);

        assertEquals(expected, result.operator);

        assertEquals(
                new ObjectProjection(asList("id1", "id2"),
                        asList(
                                new ExpressionProjection(e("s.id1")),
                                new ExpressionProjection(e("a.id2")))),
                result.projection);
    }

    @Test
    public void test_cross_apply_with_populate()
    {
        String query = "select s.id1, a.id2 from source s cross apply article a with(populate=true)";
        QueryResult result = getQueryResult(query);

        assertTrue("Alias hierarchy should be equal", source.isEqual(result.alias));

        Operator expected = new NestedLoopJoin(
                3,
                "",
                result.tableOperators.get(0),
                new CachingOperator(2, result.tableOperators.get(1)),
                null,
                DefaultRowMerger.DEFAULT,
                true,
                false);

        //        System.out.println(expected.toString(1));
        //        System.err.println(result.operator.toString(1));

        assertEquals(expected, result.operator);

        assertEquals(
                new ObjectProjection(asList("id1", "id2"),
                        asList(
                                new ExpressionProjection(e("s.id1")),
                                new ExpressionProjection(e("a.id2")))),
                result.projection);
    }

    @Test
    public void test_outer_apply()
    {
        String query = "select s.id1, a.id2 from source s outer apply article a";
        QueryResult result = getQueryResult(query);

        assertTrue("Alias hierarchy should be equal", source.isEqual(result.alias));

        Operator expected = new NestedLoopJoin(
                3,
                "",
                result.tableOperators.get(0),
                new CachingOperator(2, result.tableOperators.get(1)),
                null,
                DefaultRowMerger.DEFAULT,
                false,
                true);

        assertEquals(expected, result.operator);

        assertEquals(
                new ObjectProjection(asList("id1", "id2"),
                        asList(
                                new ExpressionProjection(e("s.id1")),
                                new ExpressionProjection(e("a.id2")))),
                result.projection);
    }

    @Test
    public void test_outer_apply_with_pushdown()
    {
        String query = "select s.id1, a.id2 from source s outer apply article a where a.active_flg";
        QueryResult result = getQueryResult(query);

        TableAlias source = TableAlias.of(null, QualifiedName.of("source"), "s", new String[] {"id1"});
        TableAlias.of(source, QualifiedName.of("article"), "a", new String[] {"active_flg", "id2"});

        assertTrue("Alias hierarchy should be equal", source.isEqual(result.alias));

        Operator expected = new NestedLoopJoin(
                4,
                "OUTER APPLY",
                result.tableOperators.get(0),
                new CachingOperator(
                        3,
                        new FilterOperator(
                                2,
                                result.tableOperators.get(1),
                                new ExpressionPredicate(e("a.active_flg = true")))),
                null,
                DefaultRowMerger.DEFAULT,
                false,
                true);

        //                System.out.println(expected.toString(1));
        //                System.err.println(result.operator.toString(1));

        assertEquals(expected, result.operator);

        assertEquals(
                new ObjectProjection(asList("id1", "id2"),
                        asList(
                                new ExpressionProjection(e("s.id1")),
                                new ExpressionProjection(e("a.id2")))),
                result.projection);
    }

    @Test
    public void test_outer_apply_not_pushdown_is_null()
    {
        String query = "select s.id1, a.id2 from source s outer apply article a where a.active_flg and a.value is null";
        QueryResult result = getQueryResult(query);

        TableAlias source = TableAlias.of(null, QualifiedName.of("source"), "s", new String[] {"id1"});
        TableAlias.of(source, QualifiedName.of("article"), "a", new String[] {"active_flg", "value", "id2"});
        
        assertTrue("Alias hierarchy should be equal", source.isEqual(result.alias));

        Operator expected = new FilterOperator(
                5,
                new NestedLoopJoin(
                        4,
                        "OUTER APPLY",
                        result.tableOperators.get(0),
                        new CachingOperator(
                                3,
                                new FilterOperator(
                                        2,
                                        result.tableOperators.get(1),
                                        new ExpressionPredicate(e("a.active_flg = true")))),
                        null,
                        DefaultRowMerger.DEFAULT,
                        false,
                        true),
                new ExpressionPredicate(e("a.value is null")));

        //        System.out.println(expected.toString(1));
        //        System.err.println(result.operator.toString(1));

        assertEquals(expected, result.operator);

        assertEquals(
                new ObjectProjection(asList("id1", "id2"),
                        asList(
                                new ExpressionProjection(e("s.id1")),
                                new ExpressionProjection(e("a.id2")))),
                result.projection);
    }

    @Test
    public void test_outer_apply_with_populate()
    {
        String query = "select s.id1, a.id2 from source s outer apply article a with (populate=true)";
        QueryResult result = getQueryResult(query);

        assertTrue("Alias hierarchy should be equal", source.isEqual(result.alias));

        Operator expected = new NestedLoopJoin(
                3,
                "",
                result.tableOperators.get(0),
                new CachingOperator(2, result.tableOperators.get(1)),
                null,
                DefaultRowMerger.DEFAULT,
                true,
                true);

        assertEquals(expected, result.operator);

        assertEquals(
                new ObjectProjection(asList("id1", "id2"),
                        asList(
                                new ExpressionProjection(e("s.id1")),
                                new ExpressionProjection(e("a.id2")))),
                result.projection);
    }
}
