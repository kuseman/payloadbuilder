package com.viskan.payloadbuilder.analyze;

import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.operator.DefaultRowMerger;
import com.viskan.payloadbuilder.operator.ExpressionHashFunction;
import com.viskan.payloadbuilder.operator.ExpressionPredicate;
import com.viskan.payloadbuilder.operator.ExpressionProjection;
import com.viskan.payloadbuilder.operator.FilterOperator;
import com.viskan.payloadbuilder.operator.HashMatch;
import com.viskan.payloadbuilder.operator.ObjectProjection;
import com.viskan.payloadbuilder.operator.Operator;
import com.viskan.payloadbuilder.parser.tree.QualifiedName;

import static com.viskan.payloadbuilder.utils.MapUtils.entry;
import static com.viskan.payloadbuilder.utils.MapUtils.ofEntries;
import static java.util.Arrays.asList;

import org.junit.Test;

/** Test of {@link OperatorBuilder} building hash match (joins) */
public class OperatorBuilderHashJoinTest extends AOperatorBuilderTest
{
    @Test
    public void test_hash_join()
    {
        String query = "select s.id1, a.id2 from source s inner join article a on s.art_id = a.art_id";
        QueryResult result = getQueryResult(query);

        // Assert aliaes
        TableAlias source = new TableAlias(null, QualifiedName.of("source"), "s", new String[] {"art_id", "id1"});
        new TableAlias(source, QualifiedName.of("article"), "a", new String[] {"art_id", "id2"});

        assertTrue("Alias hierarchy should be equal", source.isEqual(result.alias));

        Operator expected = new HashMatch(
                "",
                result.tableOperators.get(0),
                result.tableOperators.get(1),
                new ExpressionHashFunction(asList(e("s.art_id"))),
                new ExpressionHashFunction(asList(e("a.art_id"))),
                new ExpressionPredicate(e("s.art_id = a.art_id")),
                DefaultRowMerger.DEFAULT,
                false,
                false);

        //        System.out.println(expected.toString(1));
        //        System.out.println(result.operator.toString(1));

        assertEquals(expected, result.operator);

        assertEquals(
                new ObjectProjection(ofEntries(true,
                        entry("id1", new ExpressionProjection(e("s.id1"))),
                        entry("id2", new ExpressionProjection(e("a.id2"))))),
                result.projection);
    }

    @Test
    public void test_hash_join_populate()
    {
        String query = "select s.id1, a.id2 from source s inner join [article] a on a.art_id = s.art_id";
        QueryResult result = getQueryResult(query);

        // Assert aliaes
        TableAlias source = new TableAlias(null, QualifiedName.of("source"), "s", new String[] {"art_id", "id1"});
        new TableAlias(source, QualifiedName.of("article"), "a", new String[] {"art_id", "id2"});

        assertTrue("Alias hierarchy should be equal", source.isEqual(result.alias));

        Operator expected = new HashMatch(
                "",
                result.tableOperators.get(0),
                result.tableOperators.get(1),
                new ExpressionHashFunction(asList(e("s.art_id"))),
                new ExpressionHashFunction(asList(e("a.art_id"))),
                new ExpressionPredicate(e("a.art_id = s.art_id")),
                DefaultRowMerger.DEFAULT,
                true,
                false);

        //        System.err.println(expected.toString(1));
        //        System.out.println(result.operator.toString(1));

        assertEquals(expected, result.operator);

        assertEquals(
                new ObjectProjection(ofEntries(true,
                        entry("id1", new ExpressionProjection(e("s.id1"))),
                        entry("id2", new ExpressionProjection(e("a.id2"))))),
                result.projection);
    }

    @Test
    public void test_left_hash_join()
    {
        String query = "select s.id1, a.id2 from source s left join article a on s.art_id = a.art_id";
        QueryResult result = getQueryResult(query);

        // Assert aliaes
        TableAlias source = new TableAlias(null, QualifiedName.of("source"), "s", new String[] {"art_id", "id1"});
        new TableAlias(source, QualifiedName.of("article"), "a", new String[] {"art_id", "id2"});

        assertTrue("Alias hierarchy should be equal", source.isEqual(result.alias));

        Operator expected = new HashMatch(
                "",
                result.tableOperators.get(0),
                result.tableOperators.get(1),
                new ExpressionHashFunction(asList(e("s.art_id"))),
                new ExpressionHashFunction(asList(e("a.art_id"))),
                new ExpressionPredicate(e("s.art_id = a.art_id")),
                DefaultRowMerger.DEFAULT,
                false,
                true);

        //                System.err.println(expected.toString(1));
        //                System.out.println(result.operator.toString(1));

        assertEquals(expected, result.operator);

        assertEquals(
                new ObjectProjection(ofEntries(true,
                        entry("id1", new ExpressionProjection(e("s.id1"))),
                        entry("id2", new ExpressionProjection(e("a.id2"))))),
                result.projection);
    }

    @Test
    public void test_left_hash_join_with_populate()
    {
        String query = "select s.id1, a.id2 from source s left join [article] a on s.art_id = a.art_id";
        QueryResult result = getQueryResult(query);

        // Assert aliaes
        TableAlias source = new TableAlias(null, QualifiedName.of("source"), "s", new String[] {"art_id", "id1"});
        new TableAlias(source, QualifiedName.of("article"), "a", new String[] {"art_id", "id2"});

        assertTrue("Alias hierarchy should be equal", source.isEqual(result.alias));

        Operator expected = new HashMatch(
                "",
                result.tableOperators.get(0),
                result.tableOperators.get(1),
                new ExpressionHashFunction(asList(e("s.art_id"))),
                new ExpressionHashFunction(asList(e("a.art_id"))),
                new ExpressionPredicate(e("s.art_id = a.art_id")),
                DefaultRowMerger.DEFAULT,
                true,
                true);

        assertEquals(expected, result.operator);

        assertEquals(
                new ObjectProjection(ofEntries(true,
                        entry("id1", new ExpressionProjection(e("s.id1"))),
                        entry("id2", new ExpressionProjection(e("a.id2"))))),
                result.projection);
    }

    @Test
    public void test_inner_hash_join_with_filter()
    {
        String query = "select s.id1, a.id2 from source s inner join [article where note_id > 0] a on a.art_id = s.art_id and a.active_flg where s.id3 > 0 and a.id2 = 10";
        QueryResult result = getQueryResult(query);

        // Assert aliaes
        TableAlias source = new TableAlias(null, QualifiedName.of("source"), "s", new String[] {"art_id", "id3", "id1"});
        new TableAlias(source, QualifiedName.of("article"), "a", new String[] {"art_id", "active_flg", "id2", "note_id"});

        assertTrue("Alias hierarchy should be equal", source.isEqual(result.alias));

        Operator expected = new FilterOperator(
                new HashMatch(
                        "",
                        new FilterOperator(result.tableOperators.get(0), new ExpressionPredicate(e("s.id3 > 0"))),
                        new FilterOperator(result.tableOperators.get(1), new ExpressionPredicate(e("a.active_flg AND note_id > 0"))),
                        new ExpressionHashFunction(asList(e("s.art_id"))),
                        new ExpressionHashFunction(asList(e("a.art_id"))),
                        new ExpressionPredicate(e("a.art_id = s.art_id")),
                        DefaultRowMerger.DEFAULT,
                        true,
                        false),
                new ExpressionPredicate(e("a.id2 = 10")));

//        System.err.println(expected.toString(1));
//        System.out.println(result.operator.toString(1));

        assertEquals(expected, result.operator);

        assertEquals(
                new ObjectProjection(ofEntries(true,
                        entry("id1", new ExpressionProjection(e("s.id1"))),
                        entry("id2", new ExpressionProjection(e("a.id2"))))),
                result.projection);
    }
}
