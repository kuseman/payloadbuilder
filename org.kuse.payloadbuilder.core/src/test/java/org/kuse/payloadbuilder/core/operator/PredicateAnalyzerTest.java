package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static org.kuse.payloadbuilder.core.parser.QualifiedName.of;
import static org.kuse.payloadbuilder.core.utils.CollectionUtils.asSet;

import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.catalog.CatalogRegistry;
import org.kuse.payloadbuilder.core.operator.PredicateAnalyzer.AnalyzeItem;
import org.kuse.payloadbuilder.core.operator.PredicateAnalyzer.AnalyzePair;
import org.kuse.payloadbuilder.core.operator.PredicateAnalyzer.AnalyzePair.Type;
import org.kuse.payloadbuilder.core.operator.PredicateAnalyzer.AnalyzeResult;
import org.kuse.payloadbuilder.core.operator.TableAlias.TableAliasBuilder;
import org.kuse.payloadbuilder.core.parser.ComparisonExpression;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.parser.QueryParser;
import org.kuse.payloadbuilder.core.utils.CollectionUtils;

/** Test of {@link PredicateAnalyzer} */
public class PredicateAnalyzerTest extends Assert
{
    private final QueryParser parser = new QueryParser();
    private final QuerySession session = new QuerySession(new CatalogRegistry());

    @Test
    public void test_AnalyzeResult()
    {
        TableAlias root = TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("ROOT"), "ROOT")
                .children(asList(
                        TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("tableA"), "a"),
                        TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("tableS"), "s")))
                .build();

        TableAlias alias = root.getChildAliases().get(0);
        
        Pair<List<AnalyzePair>, AnalyzeResult> pair;
        AnalyzeResult result;
        result = PredicateAnalyzer.analyze(null, alias);
        assertEquals(result, AnalyzeResult.EMPTY);
        assertNull(result.getPredicate());
        pair = result.extractPushdownPairs("a");
        assertEquals(emptyList(), pair.getLeft());
        assertEquals(AnalyzeResult.EMPTY, pair.getValue());

        result = PredicateAnalyzer.analyze(e("(a.flag)"), alias);
        assertEquals(
                result(
                        pair(Type.COMPARISION, ComparisonExpression.Type.EQUAL, e("a.flag"), asSet("a"), of("flag"), e("true"), emptySet(), null)),
                result);
        assertEquals(e("(a.flag = true)"), result.getPredicate());
        pair = result.extractPushdownPairs("a");
        assertEquals(asList(
                pair(Type.COMPARISION, ComparisonExpression.Type.EQUAL, e("a.flag"), asSet("a"), of("flag"), e("true"), emptySet(), null)),
                pair.getLeft());
        assertEquals(AnalyzeResult.EMPTY, pair.getValue());

        result = PredicateAnalyzer.analyze(e("a.art_id = s.art_id"), alias);
        assertEquals(
                result(
                        pair(Type.COMPARISION, ComparisonExpression.Type.EQUAL, e("a.art_id"), asSet("a"), of("art_id"), e("s.art_id"), asSet("s"), of("art_id"))),
                result);
        assertEquals(e("a.art_id = s.art_id"), result.getPredicate());
        pair = result.extractPushdownPairs("a");
        assertEquals(emptyList(), pair.getLeft());
        assertEquals(result, pair.getValue());

        result = PredicateAnalyzer.analyze(e("a.art_id is null"), alias);
        assertEquals(
                result(
                        pair(Type.NULL, null, e("a.art_id"), asSet("a"), of("art_id"), e("a.art_id is null"), asSet(), null)),
                result);
        assertEquals(e("a.art_id is null"), result.getPredicate());
        pair = result.extractPushdownPairs("a");
        assertEquals(asList(
                pair(Type.NULL, null, e("a.art_id"), asSet("a"), of("art_id"), e("a.art_id is null"), asSet(), null)), pair.getLeft());
        assertEquals(AnalyzeResult.EMPTY, pair.getValue());
        pair = result.extractPushdownPairs("a", false);
        assertEquals(emptyList(), pair.getLeft());
        assertEquals(result, pair.getValue());

        result = PredicateAnalyzer.analyze(e("a.art_id is not null"), alias);
        assertEquals(
                result(
                        pair(Type.NOT_NULL, null, e("a.art_id"), asSet("a"), of("art_id"), e("a.art_id is not null"), asSet(), null)),
                result);
        assertEquals(e("a.art_id is not null"), result.getPredicate());
        pair = result.extractPushdownPairs("a");
        assertEquals(asList(
                pair(Type.NOT_NULL, null, e("a.art_id"), asSet("a"), of("art_id"), e("a.art_id is not null"), asSet(), null)), pair.getLeft());
        assertEquals(AnalyzeResult.EMPTY, pair.getValue());
        pair = result.extractPushdownPairs("a", false);
        assertEquals(asList(
                pair(Type.NOT_NULL, null, e("a.art_id"), asSet("a"), of("art_id"), e("a.art_id is not null"), asSet(), null)), pair.getLeft());
        assertEquals(AnalyzeResult.EMPTY, pair.getValue());

        result = PredicateAnalyzer.analyze(e("a.art_id = s.art_id and sku_id = s.sku_id and active_flg and not a.internet_flg and a.value > 100"), alias);

        assertEquals(
                result(
                        pair(Type.COMPARISION, ComparisonExpression.Type.GREATER_THAN, e("a.value"), asSet("a"), of("value"), e("100"), asSet(), null),
                        pair(Type.COMPARISION, ComparisonExpression.Type.EQUAL, e("a.internet_flg"), asSet("a"), of("internet_flg"), e("false"), asSet(), null),
                        pair(Type.COMPARISION, ComparisonExpression.Type.EQUAL, e("active_flg"), asSet(""), of("active_flg"), e("true"), asSet(), null),
                        pair(Type.COMPARISION, ComparisonExpression.Type.EQUAL, e("sku_id"), asSet(""), of("sku_id"), e("s.sku_id"), asSet("s"), of("sku_id")),
                        pair(Type.COMPARISION, ComparisonExpression.Type.EQUAL, e("a.art_id"), asSet("a"), of("art_id"), e("s.art_id"), asSet("s"), of("art_id"))),
                result);
        assertEquals(asList(
                pair(Type.COMPARISION, ComparisonExpression.Type.EQUAL, e("a.internet_flg"), asSet("a"), of("internet_flg"), e("false"), asSet(), null),
                pair(Type.COMPARISION, ComparisonExpression.Type.EQUAL, e("active_flg"), asSet(""), of("active_flg"), e("true"), asSet(), null),
                pair(Type.COMPARISION, ComparisonExpression.Type.EQUAL, e("sku_id"), asSet(""), of("sku_id"), e("s.sku_id"), asSet("s"), of("sku_id")),
                pair(Type.COMPARISION, ComparisonExpression.Type.EQUAL, e("a.art_id"), asSet("a"), of("art_id"), e("s.art_id"), asSet("s"), of("art_id"))), result.getEquiPairs("a"));
        assertEquals(e("a.art_id = s.art_id and sku_id = s.sku_id and active_flg = true and a.internet_flg = false and a.value > 100"), result.getPredicate());
        pair = result.extractPushdownPairs("a");
        assertEquals(asList(
                pair(Type.COMPARISION, ComparisonExpression.Type.GREATER_THAN, e("a.value"), asSet("a"), of("value"), e("100"), asSet(), null),
                pair(Type.COMPARISION, ComparisonExpression.Type.EQUAL, e("a.internet_flg"), asSet("a"), of("internet_flg"), e("false"), asSet(), null),
                pair(Type.COMPARISION, ComparisonExpression.Type.EQUAL, e("active_flg"), asSet(""), of("active_flg"), e("true"), asSet(), null)),
                pair.getLeft());
        assertEquals(
                result(
                        pair(Type.COMPARISION, ComparisonExpression.Type.EQUAL, e("sku_id"), asSet(""), of("sku_id"), e("s.sku_id"), asSet("s"), of("sku_id")),
                        pair(Type.COMPARISION, ComparisonExpression.Type.EQUAL, e("a.art_id"), asSet("a"), of("art_id"), e("s.art_id"), asSet("s"), of("art_id"))),
                pair.getValue());
        assertEquals(e("a.art_id = s.art_id and sku_id = s.sku_id"), pair.getValue().getPredicate());

        result = PredicateAnalyzer.analyze(e("a.art_id = s.art_id or (sku_id = s.sku_id and active_flg and not a.internet_flg and a.value > 100)"), alias);

        assertEquals(
                result(
                        pair(Type.UNDEFINED, null, e("a.art_id = s.art_id or (sku_id = s.sku_id and active_flg and not a.internet_flg and a.value > 100)"), asSet("a", "s"), null)),
                result);
    }
    
    @Test
    public void test_nested_reference()
    {
        TableAlias alias = TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("ROOT"), "ROOT")
                .children(asList(
                        TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("source"), "s"),
                        TableAliasBuilder.of(TableAlias.Type.SUBQUERY, QualifiedName.of("source"), "a")
                                .children(asList(
                                        TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("article"), "a"),
                                        TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("articleBrand"), "ab")))))
                .build();
        
        AnalyzePair p;

        // Start from source and analyze a where, a nested reference cannot be pushed down
        p = PredicateAnalyzer.AnalyzePair.of(alias.getChildAliases().get(0), e("a.ab.active_flg"));
        assertEquals(Type.COMPARISION, p.getType());
        assertEquals(ComparisonExpression.Type.EQUAL, p.getComparisonType());
        assertEquals(e("a.ab.active_flg = true"), p.getPredicate());
        assertNull(p.getColumn("b"));
        assertNull(p.getColumn("s"));
        assertFalse(p.isPushdown("a"));
        assertFalse(p.isPushdown("b"));
        assertFalse(p.isEqui("s"));
    }

    @Test
    public void test_AnalyzePair()
    {
        TableAlias root = TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("ROOT"), "ROOT")
                .children(asList(
                        TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("SubQuery"), "aa")
                                .children(asList(
                                        TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("tableA"), "articleAttribute"),
                                        TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("tableS"), "a1")))))
                .build();
    
        TableAlias alias = root.getChildAliases().get(0);
        
        AnalyzePair p;

        p = PredicateAnalyzer.AnalyzePair.of(alias, e("aa.art_id is null"));
        assertEquals(Type.NULL, p.getType());
        assertNull(p.getComparisonType());
        assertEquals(e("aa.art_id is null"), p.getPredicate());
        assertNull(p.getColumn("b"));
        assertNull(p.getColumn("s"));
        assertTrue(p.isPushdown("aa"));
        assertFalse(p.isPushdown("b"));
        assertFalse(p.isEqui("s"));

        p = PredicateAnalyzer.AnalyzePair.of(alias, e("count(aa.a1) > 0"));
        assertEquals(Type.COMPARISION, p.getType());
        assertEquals(ComparisonExpression.Type.GREATER_THAN, p.getComparisonType());
        assertEquals(e("count(aa.a1) > 0"), p.getPredicate());
        assertNull(p.getColumn("b"));
        assertNull(p.getColumn("s"));
        assertFalse(p.isPushdown("aa"));
        assertFalse(p.isPushdown("b"));
        assertFalse(p.isEqui("s"));

        p = PredicateAnalyzer.AnalyzePair.of(alias, e("aa.a1 > 0"));
        assertEquals(Type.COMPARISION, p.getType());
        assertEquals(ComparisonExpression.Type.GREATER_THAN, p.getComparisonType());
        assertEquals(e("aa.a1 > 0"), p.getPredicate());
        assertNull(p.getColumn("b"));
        assertNull(p.getColumn("s"));
        assertFalse(p.isPushdown("aa"));
        assertFalse(p.isPushdown("b"));
        assertFalse(p.isEqui("s"));

        root = TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("ROOT"), "ROOT")
                .children(asList(
                        TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("tableS"), "s"),
                        TableAliasBuilder.of(TableAlias.Type.SUBQUERY, QualifiedName.of("tableA"), "a")
                                .children(asList(
                                        TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("tableA_A"), "a_a"))),
                        TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("tableB"), "b"),
                        TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("tableC"), "c")

                ))
                .build();

        // Start from a_a and traverse up to parent
        alias = root.getChildAliases().get(1).getChildAliases().get(0);
        p = PredicateAnalyzer.AnalyzePair.of(alias, e("s.id"));
        assertEquals(Type.COMPARISION, p.getType());
        assertEquals(ComparisonExpression.Type.EQUAL, p.getComparisonType());
        assertEquals(e("s.id = true"), p.getPredicate());
        assertNull(p.getColumn("b"));
        assertEquals("id", p.getColumn("s"));
        assertTrue(p.isPushdown("s"));
        assertFalse(p.isPushdown("a_a"));
        assertFalse(p.isPushdown("b"));
        assertTrue(p.isEqui("s"));
        
        alias = root.getChildAliases().get(0);
        
        // Alias access
        p = PredicateAnalyzer.AnalyzePair.of(alias, e("s"));
        assertEquals(Type.COMPARISION, p.getType());
        assertEquals(ComparisonExpression.Type.EQUAL, p.getComparisonType());
        assertEquals(e("s = true"), p.getPredicate());
        assertNull(p.getColumn("b"));
        assertNull(p.getColumn("s"));
        assertFalse(p.isPushdown("s"));
        assertFalse(p.isPushdown("b"));
        assertFalse(p.isEqui("s"));

        // Child access
        p = PredicateAnalyzer.AnalyzePair.of(alias, e("a"));
        assertEquals(Type.COMPARISION, p.getType());
        assertEquals(ComparisonExpression.Type.EQUAL, p.getComparisonType());
        assertEquals(e("a = true"), p.getPredicate());
        assertNull(p.getColumn("b"));
        assertNull(p.getColumn("s"));
        assertFalse(p.isPushdown("s"));
        assertFalse(p.isPushdown("b"));
        assertFalse(p.isEqui("s"));

        // Child access
        p = PredicateAnalyzer.AnalyzePair.of(alias, e("a.a_a.col"));
        assertEquals(Type.COMPARISION, p.getType());
        assertEquals(ComparisonExpression.Type.EQUAL, p.getComparisonType());
        assertEquals(e("a.a_a.col = true"), p.getPredicate());
        assertNull(p.getColumn("b"));
        assertNull(p.getColumn("s"));
        assertFalse(p.isPushdown("s"));
        assertFalse(p.isPushdown("b"));
        assertFalse(p.isEqui("s"));

        p = PredicateAnalyzer.AnalyzePair.of(alias, e("@val > s.art_id"));
        assertEquals(Type.COMPARISION, p.getType());
        assertEquals(ComparisonExpression.Type.GREATER_THAN, p.getComparisonType());
        assertEquals(e("@val > s.art_id"), p.getPredicate());
        assertNull(p.getColumn("b"));
        assertEquals("art_id", p.getColumn("s"));
        assertTrue(p.isPushdown("s"));
        assertFalse(p.isPushdown("b"));
        assertFalse(p.isEqui("s"));
        assertEquals(Pair.of(e("s.art_id"), e("@val")), p.getExpressionPair("s"));
        try
        {
            p.getExpressionPair("a");
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage().contains("No expressions could be found"));
        }

        p = PredicateAnalyzer.AnalyzePair.of(alias, e("s.value + value2 > 10"));
        assertEquals(Type.COMPARISION, p.getType());
        assertEquals(ComparisonExpression.Type.GREATER_THAN, p.getComparisonType());
        assertEquals(e("s.value + value2 > 10"), p.getPredicate());
        assertNull(p.getColumn("b"));
        assertNull(p.getColumn("s"));
        assertTrue(p.isPushdown("s"));
        assertFalse(p.isPushdown("b"));
        assertEquals(Pair.of(e("s.value + value2"), e("10")), p.getExpressionPair("s"));

        p = PredicateAnalyzer.AnalyzePair.of(alias, e("s.value + value2 + c.value > 10"));
        assertEquals(Type.COMPARISION, p.getType());
        assertEquals(ComparisonExpression.Type.GREATER_THAN, p.getComparisonType());
        assertEquals(e("s.value + value2 + c.value > 10"), p.getPredicate());
        assertNull(p.getColumn("b"));
        assertNull(p.getColumn("s"));
        assertFalse(p.isPushdown("s"));
        assertFalse(p.isPushdown("b"));

        p = PredicateAnalyzer.AnalyzePair.of(alias, e("hash(a.id) = hash(s.id)"));
        assertEquals(Type.COMPARISION, p.getType());
        assertEquals(ComparisonExpression.Type.EQUAL, p.getComparisonType());
        assertEquals(e("hash(a.id) = hash(s.id)"), p.getPredicate());
        assertNull(p.getColumn("b"));
        assertNull(p.getColumn("s"));
        assertFalse(p.isPushdown("s"));
        assertFalse(p.isPushdown("b"));
        assertTrue(p.isEqui("a"));
        assertTrue(p.isEqui("s"));
        assertFalse(p.isEqui("b"));
        assertEquals(Pair.of(e("hash(a.id)"), e("hash(s.id)")), p.getExpressionPair("a"));
        assertEquals(Pair.of(e("hash(s.id)"), e("hash(a.id)")), p.getExpressionPair("s"));

        p = PredicateAnalyzer.AnalyzePair.of(alias, e("a.art_id = art_id_rel"));
        assertEquals(Type.COMPARISION, p.getType());
        assertEquals(ComparisonExpression.Type.EQUAL, p.getComparisonType());
        assertEquals(e("a.art_id = art_id_rel"), p.getPredicate());
        assertEquals("art_id", p.getColumn("a"));
        assertEquals("art_id_rel", p.getColumn("s"));
        assertFalse(p.isPushdown("s"));
        assertTrue(p.isPushdown("a"));
        assertFalse(p.isPushdown("b"));

        p = PredicateAnalyzer.AnalyzePair.of(alias, e("art_id = s.art_id_rel"));
        assertEquals(Type.COMPARISION, p.getType());
        assertEquals(ComparisonExpression.Type.EQUAL, p.getComparisonType());
        assertEquals(e("art_id = s.art_id_rel"), p.getPredicate());
        assertEquals("art_id", p.getColumn("a"));
        assertEquals("art_id_rel", p.getColumn("s"));
        assertTrue(p.isPushdown("s"));
        assertFalse(p.isPushdown("a"));
        assertFalse(p.isPushdown("b"));

        p = PredicateAnalyzer.AnalyzePair.of(alias, e("s.active"));
        assertEquals(Type.COMPARISION, p.getType());
        assertEquals(ComparisonExpression.Type.EQUAL, p.getComparisonType());
        assertEquals(e("s.active = true"), p.getPredicate());
        assertNull(p.getColumn("b"));
        assertEquals("active", p.getColumn("s"));
        assertTrue(p.isPushdown("s"));
        assertFalse(p.isPushdown("b"));
        assertTrue(p.isEqui("s"));

        p = PredicateAnalyzer.AnalyzePair.of(alias, e("active"));
        assertEquals(Type.COMPARISION, p.getType());
        assertEquals(ComparisonExpression.Type.EQUAL, p.getComparisonType());
        assertEquals(e("active = true"), p.getPredicate());
        assertEquals("active", p.getColumn("b"));
        assertEquals("active", p.getColumn("s"));
        assertTrue(p.isPushdown("s"));
        assertTrue(p.isPushdown("b"));

        p = PredicateAnalyzer.AnalyzePair.of(alias, e("s.id = s.id2"));
        assertEquals(Type.UNDEFINED, p.getType());
        assertNull(p.getComparisonType());
        assertEquals(e("s.id = s.id2"), p.getPredicate());
        assertNull(p.getColumn("b"));
        assertNull(p.getColumn("s"));
        assertEquals(CollectionUtils.asSet("s"), p.getLeft().aliases);
        assertTrue(p.isPushdown("s"));
        assertFalse(p.isPushdown("a"));

        p = PredicateAnalyzer.AnalyzePair.of(alias, e("s.art_id > @val"));
        assertEquals(Type.COMPARISION, p.getType());
        assertEquals(ComparisonExpression.Type.GREATER_THAN, p.getComparisonType());
        assertEquals(e("s.art_id > @val"), p.getPredicate());
        assertNull(p.getColumn("b"));
        assertEquals("art_id", p.getColumn("s"));
        assertTrue(p.isPushdown("s"));

        p = PredicateAnalyzer.AnalyzePair.of(alias, e("a.art_id = s.art_id + a.id"));
        assertEquals(Type.COMPARISION, p.getType());
        assertEquals(ComparisonExpression.Type.EQUAL, p.getComparisonType());
        assertEquals(e("a.art_id = s.art_id + a.id"), p.getPredicate());
        assertNull(p.getColumn("s"));
        assertEquals("art_id", p.getColumn("a"));
        assertFalse(p.isPushdown("a"));
        assertFalse(p.isPushdown("s"));
        assertFalse(p.isPushdown("b"));
        assertFalse(p.isEqui("a"));
        assertFalse(p.isEqui("s"));
        assertFalse(p.isEqui("b"));

        p = PredicateAnalyzer.AnalyzePair.of(alias, e("a.art_id = art_id_rel"));
        assertEquals(Type.COMPARISION, p.getType());
        assertEquals(ComparisonExpression.Type.EQUAL, p.getComparisonType());
        assertEquals(e("a.art_id = art_id_rel"), p.getPredicate());
        assertEquals("art_id", p.getColumn("a"));
        assertEquals("art_id_rel", p.getColumn("s"));
        assertFalse(p.isEqui("a"));
        assertTrue(p.isEqui("s"));
        assertTrue(p.isEqui("b"));

        p = PredicateAnalyzer.AnalyzePair.of(alias, e("a.b.art_id = s.art_id_rel"));
        assertEquals(Type.COMPARISION, p.getType());
        assertEquals(ComparisonExpression.Type.EQUAL, p.getComparisonType());
        assertEquals(e("a.b.art_id = s.art_id_rel"), p.getPredicate());
        assertNull(p.getColumn("a"));
        assertEquals("art_id_rel", p.getColumn("s"));
        assertTrue(p.isEqui("a"));
        assertFalse(p.isEqui("b"));

        p = PredicateAnalyzer.AnalyzePair.of(alias, e("a.art_id = s.d.art_id_rel"));
        assertEquals(Type.COMPARISION, p.getType());
        assertEquals(ComparisonExpression.Type.EQUAL, p.getComparisonType());
        assertEquals(e("a.art_id = s.d.art_id_rel"), p.getPredicate());
        assertEquals("art_id", p.getColumn("a"));
        assertNull(p.getColumn("s"));

        p = PredicateAnalyzer.AnalyzePair.of(alias, e("a.art_id >= s.art_id"));
        assertEquals(Type.COMPARISION, p.getType());
        assertEquals(ComparisonExpression.Type.GREATER_THAN_EQUAL, p.getComparisonType());
        assertEquals(e("a.art_id >= s.art_id"), p.getPredicate());
        assertNull(p.getColumn("b"));
        assertEquals("art_id", p.getColumn("a"));
        assertEquals("art_id", p.getColumn("s"));

        p = PredicateAnalyzer.AnalyzePair.of(alias, e("severity in (200, 300)"));
        assertEquals(Type.IN, p.getType());
        assertNull(p.getComparisonType());
        assertEquals(e("severity in (200, 300)"), p.getPredicate());
        assertEquals("severity", p.getColumn("b"));
        assertEquals("severity", p.getColumn("a"));
        assertEquals("severity", p.getColumn("s"));

        p = PredicateAnalyzer.AnalyzePair.of(alias, e("a.art_id in (s.id, b.id)"));
        assertEquals(Type.IN, p.getType());
        assertNull(p.getComparisonType());
        assertEquals(e("a.art_id in (s.id, b.id)"), p.getPredicate());
        assertNull(p.getColumn("b"));
        assertEquals("art_id", p.getColumn("a"));
        assertNull(p.getColumn("s"));

        p = PredicateAnalyzer.AnalyzePair.of(alias, e("@val in (200, 300)"));
        assertEquals(Type.UNDEFINED, p.getType());
        assertNull(p.getComparisonType());
        assertEquals(e("@val in (200, 300)"), p.getPredicate());
        assertNull(p.getColumn("b"));
        assertNull(p.getColumn("a"));
        assertNull(p.getColumn("s"));

        p = PredicateAnalyzer.AnalyzePair.of(alias, e("not (a.art_id > s.art_id)"));
        assertEquals(Type.UNDEFINED, p.getType());
        assertNull(p.getComparisonType());
        assertEquals(e("not (a.art_id > s.art_id)"), p.getPredicate());
        assertNull(p.getColumn("b"));
        assertNull(p.getColumn("a"));
        assertNull(p.getColumn("s"));

        p = PredicateAnalyzer.AnalyzePair.of(alias, e("not a.flag"));
        assertEquals(Type.COMPARISION, p.getType());
        assertEquals(ComparisonExpression.Type.EQUAL, p.getComparisonType());
        assertEquals(e("a.flag = false"), p.getPredicate());
        assertNull(p.getColumn("b"));
        assertEquals("flag", p.getColumn("a"));
        assertNull(p.getColumn("s"));
        assertTrue(p.isPushdown("a"));
        assertFalse(p.isPushdown("s"));
    }

    private AnalyzeResult result(AnalyzePair... pairs)
    {
        return new AnalyzeResult(asList(pairs));
    }

    private AnalyzePair pair(
            AnalyzePair.Type type,
            ComparisonExpression.Type comparisonType,
            Expression leftExpression,
            Set<String> leftAliases,
            QualifiedName leftQname)
    {
        return new AnalyzePair(
                type,
                comparisonType,
                new AnalyzeItem(leftExpression, leftAliases, leftQname),
                null);
    }

    private AnalyzePair pair(
            AnalyzePair.Type type,
            ComparisonExpression.Type comparisonType,
            Expression leftExpression,
            Set<String> leftAliases,
            QualifiedName leftQname,
            Expression rightExpression,
            Set<String> rightAliases,
            QualifiedName rightQname)
    {
        return new AnalyzePair(
                type,
                comparisonType,
                new AnalyzeItem(leftExpression, leftAliases, leftQname),
                new AnalyzeItem(rightExpression, rightAliases, rightQname));
    }

    private Expression e(String expression)
    {
        return parser.parseExpression(session.getCatalogRegistry(), expression);
    }
}
