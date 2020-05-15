package com.viskan.payloadbuilder.analyze;

import com.viskan.payloadbuilder.analyze.PredicateAnalyzer.AnalyzeItem;
import com.viskan.payloadbuilder.analyze.PredicateAnalyzer.AnalyzeResult;
import com.viskan.payloadbuilder.catalog.CatalogRegistry;
import com.viskan.payloadbuilder.parser.QueryParser;
import com.viskan.payloadbuilder.parser.tree.Expression;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

import org.junit.Assert;
import org.junit.Test;

/** Test of {@link PredicateAnalyzer} */
public class PredicateAnalyzerTest extends Assert
{
    private final QueryParser parser = new QueryParser();
    private final CatalogRegistry catalogRegistry = new CatalogRegistry();

    @Test
    public void test()
    {
        Expression e;
        AnalyzeResult actual;

        e = e("NOT (a.art_id = s.art_id)");
        actual = PredicateAnalyzer.analyze(e);
        assertEquals(e, actual.getPredicate());
        assertEquals(result(e, item(null, null, e("NOT (a.art_id = s.art_id)"), null, null, null)), actual);
        assertNull(actual.extractPushdownPredicate("a", false));
        assertEquals(emptyList(), actual.getEquiItems("a"));
        assertEquals(e, actual.getPredicate());

        e = e("a.art_id = s.art_id");
        actual = PredicateAnalyzer.analyze(e);
        assertEquals(result(e, item("a", "art_id", e("a.art_id"), "s", "art_id", e("s.art_id"))), actual);
        assertEquals(asList(item("a", "art_id", e("a.art_id"), "s", "art_id", e("s.art_id"))), actual.getEquiItems("a"));
        assertNull(actual.extractPushdownPredicate("a", false));

        e = e("art_id = s.art_id");
        actual = PredicateAnalyzer.analyze(e);
        assertEquals(result(e, item("", "art_id", e("art_id"), "s", "art_id", e("s.art_id"))), actual);
        assertEquals(e, actual.getPredicate());

        e = e("s.art_id = a.art_id");
        actual = PredicateAnalyzer.analyze(e);
        assertEquals(result(e, item("s", "art_id", e("s.art_id"), "a", "art_id", e("a.art_id"))), actual);

        e = e("a.art_id = s.art_id OR a.active_flg");
        actual = PredicateAnalyzer.analyze(e);
        assertEquals(result(e, item(null, null, e("a.art_id = s.art_id OR a.active_flg"), null, null, null)), actual);

        e = e("a.art_id = s.art_id + a.id");
        actual = PredicateAnalyzer.analyze(e);
        assertEquals(result(e, item("a", "art_id", e("a.art_id"), null, null, e("s.art_id + a.id"))), actual);

        e = e("a.art_id + s.id = s.art_id");
        actual = PredicateAnalyzer.analyze(e);
        assertEquals(result(e, item(null, null, e("a.art_id + s.id"), "s", "art_id", e("s.art_id"))), actual);
        assertEquals(emptyList(), actual.getEquiItems("a"));
        assertEquals(emptyList(), actual.getEquiItems("s"));
        
        e = e("a.art_id + a.idx_id = s.art_id");
        actual = PredicateAnalyzer.analyze(e);
        assertEquals(result(e, item("a", null, e("a.art_id + a.idx_id"), "s", "art_id", e("s.art_id"))), actual);
        assertEquals(asList(item("a", null, e("a.art_id + a.idx_id"), "s", "art_id", e("s.art_id"))), actual.getEquiItems("a"));
        assertEquals(asList(item("a", null, e("a.art_id + a.idx_id"), "s", "art_id", e("s.art_id"))), actual.getEquiItems("s"));

        e = e("a.art_id = s.art_id AND s.sku_id = a.sku_id");
        actual = PredicateAnalyzer.analyze(e);
        assertEquals(result(e,
                item("a", "art_id", e("a.art_id"), "s", "art_id", e("s.art_id")),
                item("s", "sku_id", e("s.sku_id"), "a", "sku_id", e("a.sku_id"))), actual);
        assertEquals(asList(
                item("a", "art_id", e("a.art_id"), "s", "art_id", e("s.art_id")),
                item("s", "sku_id", e("s.sku_id"), "a", "sku_id", e("a.sku_id"))), 
                actual.getEquiItems("s"));
        assertEquals(asList(
                item("a", "art_id", e("a.art_id"), "s", "art_id", e("s.art_id")),
                item("s", "sku_id", e("s.sku_id"), "a", "sku_id", e("a.sku_id"))), 
                actual.getEquiItems("a"));

        e = e("a.art_id > s.art_id");
        actual = PredicateAnalyzer.analyze(e);
        assertEquals(result(e, item(null, null, e("a.art_id > s.art_id"), null, null, null)), actual);
        assertEquals(e, actual.getPredicate());

        e = e("aa.art_id = a.art_id AND s.id");
        actual = PredicateAnalyzer.analyze(e);
        assertEquals(result(e,
                item("aa", "art_id", e("aa.art_id"), "a", "art_id", e("a.art_id")),
                item("s", "id", e("s.id"), null, null, null)), actual);
        assertEquals(e("s.id"), actual.extractPushdownPredicate("s", false));

        e = e("aa.art_id = a.art_id AND active_flg");
        actual = PredicateAnalyzer.analyze(e);
        assertEquals(result(e,
                item("aa", "art_id", e("aa.art_id"), "a", "art_id", e("a.art_id")),
                item("", "active_flg", e("active_flg"), null, null, null)), actual);
        assertEquals(e("active_flg"), actual.extractPushdownPredicate("s", true));
        assertEquals(e("aa.art_id = a.art_id"), actual.getPredicate());
        
        e = e("a.active_flg = 1 AND aa.art_id = a.art_id AND s.id = aa.id2 AND 1 = a.internet_flg");
        actual = PredicateAnalyzer.analyze(e);
        assertEquals(e, actual.getPredicate());
        assertEquals(result(e,
                item(null, null, e("1"), "a", "internet_flg", e("a.internet_flg")),
                item("s", "id", e("s.id"), "aa", "id2", e("aa.id2")),
                item("a", "active_flg", e("a.active_flg"), null, null, e("1")),
                item("aa", "art_id", e("aa.art_id"), "a", "art_id", e("a.art_id"))), actual);
        assertEquals(e("1 = a.internet_flg AND a.active_flg = 1"), actual.extractPushdownPredicate("a", false));
        assertEquals(e("s.id = aa.id2 AND aa.art_id = a.art_id"), actual.getPredicate());

        e = e("aa.art_id = a.art_id AND (a.active_flg OR aa.active_flg)");
        actual = PredicateAnalyzer.analyze(e);
        assertEquals(result(e,
                item("aa", "art_id", e("aa.art_id"), "a", "art_id", e("a.art_id")),
                item(null, null, e("a.active_flg OR aa.active_flg"), null, null, null)), actual);

        e = e("aa.art_id + aa.sku_id = a.art_id AND (a.active_flg OR aa.active_flg)");
        actual = PredicateAnalyzer.analyze(e);
        assertEquals(result(e,
                item("aa", null, e("aa.art_id + aa.sku_id"), "a", "art_id", e("a.art_id")),
                item(null, null, e("a.active_flg OR aa.active_flg"), null, null, null)), actual);

        e = e("aa.art_id = a.sku_id + a.art_id AND (a.active_flg OR aa.active_flg)");
        actual = PredicateAnalyzer.analyze(e);
        assertEquals(result(e,
                item("aa", "art_id", e("aa.art_id"), "a", null, e("a.sku_id + a.art_id")),
                item(null, null, e("a.active_flg OR aa.active_flg"), null, null, null)), actual);

        e = e("ap.art_id = aa.art_id AND ap.country_id = 0");
        actual = PredicateAnalyzer.analyze(e);
        assertEquals(e, actual.getPredicate());
        assertEquals(result(e,
                item("ap", "art_id", e("ap.art_id"), "aa", "art_id", e("aa.art_id")),
                item("ap", "country_id", e("ap.country_id"), null, null, e("0"))), actual);
        assertEquals(e("ap.country_id = 0"), actual.extractPushdownPredicate("ap", false));
        assertEquals(e("ap.art_id = aa.art_id"), actual.getPredicate());

        e = e("ap.art_id = aa.art_id AND 0 = ap.country_id");
        actual = PredicateAnalyzer.analyze(e);
        assertEquals(e, actual.getPredicate());
        assertEquals(result(e,
                item("ap", "art_id", e("ap.art_id"), "aa", "art_id", e("aa.art_id")),
                item(null, null, e("0"), "ap", "country_id", e("ap.country_id"))), actual);
        assertEquals(e("0 = ap.country_id"), actual.extractPushdownPredicate("ap", false));
        assertEquals(e("ap.art_id = aa.art_id"), actual.getPredicate());

        e = e("a.active_flg");
        actual = PredicateAnalyzer.analyze(e);
        assertEquals(e, actual.getPredicate());
        assertEquals(result(e,
                item("a", "active_flg", e("a.active_flg"), null, null, null)), actual);
        assertEquals(e("a.active_flg"), actual.extractPushdownPredicate("a", false));
        assertEquals(null, actual.getPredicate());
    }

    private AnalyzeResult result(Expression predicate, AnalyzeItem... items)
    {
        return new AnalyzeResult(predicate, asList(items));
    }

    private AnalyzeItem item(
            String leftAlias,
            String leftColumn,
            Expression leftExpression,
            String rightAlias,
            String rightColumn,
            Expression rightExpression)
    {
        return new AnalyzeItem(leftAlias, leftColumn, leftExpression, rightAlias, rightColumn, rightExpression);
    }

    private Expression e(String expression)
    {
        return parser.parseExpression(catalogRegistry, expression);
    }
}
