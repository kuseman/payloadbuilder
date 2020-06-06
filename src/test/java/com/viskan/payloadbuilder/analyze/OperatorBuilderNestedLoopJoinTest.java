package com.viskan.payloadbuilder.analyze;

import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.operator.CachingOperator;
import com.viskan.payloadbuilder.operator.DefaultRowMerger;
import com.viskan.payloadbuilder.operator.ExpressionPredicate;
import com.viskan.payloadbuilder.operator.ExpressionProjection;
import com.viskan.payloadbuilder.operator.FilterOperator;
import com.viskan.payloadbuilder.operator.NestedLoopJoin;
import com.viskan.payloadbuilder.operator.ObjectProjection;
import com.viskan.payloadbuilder.operator.Operator;
import com.viskan.payloadbuilder.parser.tree.QualifiedName;

import static com.viskan.payloadbuilder.utils.MapUtils.entry;
import static com.viskan.payloadbuilder.utils.MapUtils.ofEntries;

import org.junit.Test;

/** Test building of nested loops */
public class OperatorBuilderNestedLoopJoinTest extends AOperatorBuilderTest
{
    @Test
    public void test_nested_loop_with_pushdown()
    {
        String query = "select s.id1, a.id2 from source s inner join article a on a.active_flg and (a.art_id = s.art_id or s.id1 > 0)";
        QueryResult result = getQueryResult(query);

        TableAlias source = new TableAlias(null, QualifiedName.of("source"), "s", new String[] {"art_id", "id1"});
        new TableAlias(source, QualifiedName.of("article"), "a", new String[] { "art_id", "active_flg", "id2" });
        assertTrue(source.isEqual(result.alias));

        Operator expected = new NestedLoopJoin(
                "",
                result.tableOperators.get(0),
                new CachingOperator(new FilterOperator(result.tableOperators.get(1), new ExpressionPredicate(e("a.active_flg")))),
                new ExpressionPredicate(e("a.art_id = s.art_id or s.id1 > 0")),
                DefaultRowMerger.DEFAULT,
                false,
                false);

//        System.out.println(expected.toString(1));
//        System.err.println( result.operator.toString(1));
        
        assertEquals(expected, result.operator);

        assertEquals(new ObjectProjection(ofEntries(true,
                entry("id1", new ExpressionProjection(parser.parseExpression(catalogRegistry, "s.id1"))),
                entry("id2", new ExpressionProjection(parser.parseExpression(catalogRegistry, "a.id2"))))),
                result.projection);
    }
    
    @Test
    public void test_nested_loop_with_populate_and_pushdown()
    {
        String query = "select s.id1, a.id2 from source s inner join [ article a where a.internet_flg ] a on a.active_flg and (a.art_id = s.art_id or s.id1 > 0)";
        QueryResult result = getQueryResult(query);

        TableAlias source = new TableAlias(null, QualifiedName.of("source"), "s", new String[] {"art_id", "id1"});
        new TableAlias(source, QualifiedName.of("article"), "a", new String[] { "internet_flg", "art_id", "active_flg", "id2" });
        assertTrue(source.isEqual(result.alias));

        Operator expected = new NestedLoopJoin(
                "",
                result.tableOperators.get(0),
                new CachingOperator(new FilterOperator(result.tableOperators.get(1), new ExpressionPredicate(e("a.active_flg AND a.internet_flg")))),
                new ExpressionPredicate(e("a.art_id = s.art_id or s.id1 > 0")),
                DefaultRowMerger.DEFAULT,
                true,
                false);
        
//        System.out.println(expected.toString(1));
//        System.err.println( result.operator.toString(1));
        
        assertEquals(expected, result.operator);

        assertEquals(new ObjectProjection(ofEntries(true,
                entry("id1", new ExpressionProjection(parser.parseExpression(catalogRegistry, "s.id1"))),
                entry("id2", new ExpressionProjection(parser.parseExpression(catalogRegistry, "a.id2"))))),
                result.projection);
    }

}
