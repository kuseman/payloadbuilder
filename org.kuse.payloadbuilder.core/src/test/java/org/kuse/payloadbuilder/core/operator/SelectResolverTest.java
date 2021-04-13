package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.kuse.payloadbuilder.core.utils.CollectionUtils.asSet;
import static org.kuse.payloadbuilder.core.utils.MapUtils.entry;
import static org.kuse.payloadbuilder.core.utils.MapUtils.ofEntries;

import org.junit.Assert;
import org.junit.Test;
import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.catalog.CatalogRegistry;
import org.kuse.payloadbuilder.core.operator.TableAlias.TableAliasBuilder;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.parser.QualifiedReferenceExpression.ResolvePath;
import org.kuse.payloadbuilder.core.parser.QueryParser;

/** Test {@link SelectResolver} */
public class SelectResolverTest extends Assert
{
    private final QueryParser parser = new QueryParser();
    private final QuerySession session = new QuerySession(new CatalogRegistry());

    @SuppressWarnings("deprecation")
    @Test
    public void test()
    {
        TableAlias root = TableAliasBuilder.of(-1, TableAlias.Type.TABLE, QualifiedName.of("ROOT"), "ROOT")
                .children(asList(
                        TableAliasBuilder.of(0, TableAlias.Type.TABLE, QualifiedName.of("source"), "s"),
                        TableAliasBuilder.of(1, TableAlias.Type.TABLE, QualifiedName.of("article"), "a"),
                        TableAliasBuilder.of(2, TableAlias.Type.SUBQUERY, QualifiedName.of("SubQuery"), "aa")
                                .children(asList(
                                        TableAliasBuilder.of(-1, TableAlias.Type.TABLE, QualifiedName.of("ROOT"), "ROOT")
                                                .children(asList(
                                                        TableAliasBuilder.of(3, TableAlias.Type.TABLE, QualifiedName.of("articleAttribute"), "aa"),
                                                        TableAliasBuilder.of(4, TableAlias.Type.TABLE, QualifiedName.of("articlePrice"), "ap"),
                                                        TableAliasBuilder.of(5, TableAlias.Type.TABLE, QualifiedName.of("articleBalance"), "ab")

                                                )))),
                        TableAliasBuilder.of(6, TableAlias.Type.TABLE, QualifiedName.of("aricleBrand"), "aBrand")))
                .build();

        TableAlias source = root.getChildAliases().get(0);
        TableAlias article = root.getChildAliases().get(1);
        TableAlias subArticleAttribute = root.getChildAliases().get(2);
        TableAlias articleAttribute = subArticleAttribute.getChildAliases().get(0).getChildAliases().get(0);
        TableAlias articlePrice = subArticleAttribute.getChildAliases().get(0).getChildAliases().get(1);
        TableAlias articleBrand = root.getChildAliases().get(3);

        SelectResolver.Context actual;
        Expression e;

        e = e("a.col");
        actual = SelectResolver.resolve(e, source);
        assertEquals(ofEntries(entry(article, asSet("col"))), actual.getColumnsByAlias());
        assertEquals(asList(new ResolvePath(-1, 1, asList("col"))), actual.getResolvedQualifiers().get(0).getResolvePaths());

        e = e("col");
        actual = SelectResolver.resolve(e, source);
        assertEquals(ofEntries(entry(source, asSet("col"))), actual.getColumnsByAlias());
        assertEquals(asList(new ResolvePath(-1, 0, asList("col"))), actual.getResolvedQualifiers().get(0).getResolvePaths());

        e = e("aa");
        actual = SelectResolver.resolve(e, source);
        assertEquals(emptyMap(), actual.getColumnsByAlias());
        assertEquals(asList(new ResolvePath(-1, 2, emptyList())), actual.getResolvedQualifiers().get(0).getResolvePaths());

        e = e("aa.ap");
        actual = SelectResolver.resolve(e, source);
        assertEquals(emptyMap(), actual.getColumnsByAlias());
        assertEquals(asList(new ResolvePath(-1, 4, emptyList())), actual.getResolvedQualifiers().get(0).getResolvePaths());

        e = e("aa.map(x -> x)");
        actual = SelectResolver.resolve(e, source);
        assertEquals(emptyMap(), actual.getColumnsByAlias());
        // aa
        assertEquals(asList(new ResolvePath(-1, 2, emptyList())), actual.getResolvedQualifiers().get(0).getResolvePaths());
        // x
        assertEquals(0, actual.getResolvedQualifiers().get(1).getLambdaId());
        assertEquals(asList(new ResolvePath(-1, -1, emptyList())), actual.getResolvedQualifiers().get(1).getResolvePaths());

        e = e("aa.map(x -> concat(x, ','))");
        actual = SelectResolver.resolve(e, source);
        assertEquals(emptyMap(), actual.getColumnsByAlias());
        // aa
        assertEquals(asList(new ResolvePath(-1, 2, emptyList())), actual.getResolvedQualifiers().get(0).getResolvePaths());
        // x
        assertEquals(0, actual.getResolvedQualifiers().get(1).getLambdaId());
        assertEquals(asList(new ResolvePath(-1, -1, emptyList())), actual.getResolvedQualifiers().get(1).getResolvePaths());

        e = e("aa.map(x -> x.ap)");
        actual = SelectResolver.resolve(e, source);
        assertEquals(emptyMap(), actual.getColumnsByAlias());
        // aa
        assertEquals(asList(new ResolvePath(-1, 2, emptyList())), actual.getResolvedQualifiers().get(0).getResolvePaths());
        // x.ap
        assertEquals(0, actual.getResolvedQualifiers().get(1).getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 4, emptyList())), actual.getResolvedQualifiers().get(1).getResolvePaths());

        e = e("aa.map(x -> x.ap.price_sales)");
        actual = SelectResolver.resolve(e, source);
        assertEquals(ofEntries(entry(articlePrice, asSet("price_sales"))), actual.getColumnsByAlias());
        // aa
        assertEquals(asList(new ResolvePath(-1, 2, emptyList())), actual.getResolvedQualifiers().get(0).getResolvePaths());
        // x.ap.price_sales
        assertEquals(0, actual.getResolvedQualifiers().get(1).getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 4, asList("price_sales"))), actual.getResolvedQualifiers().get(1).getResolvePaths());

        e = e("unionall(aa, aa.ap)");
        actual = SelectResolver.resolve(e, source);
        assertEquals(emptyMap(), actual.getColumnsByAlias());
        // aa
        assertEquals(asList(new ResolvePath(-1, 2, emptyList())), actual.getResolvedQualifiers().get(0).getResolvePaths());
        // aa.ap
        assertEquals(asList(new ResolvePath(-1, 4, emptyList())), actual.getResolvedQualifiers().get(1).getResolvePaths());

        // Traverse down and then up to root again
        e = e("unionall(aa, aa.ap).map(x -> x.s.art_id)");
        actual = SelectResolver.resolve(e, source);
        assertEquals(ofEntries(entry(source, asSet("art_id"))), actual.getColumnsByAlias());
        // aa
        assertEquals(asList(new ResolvePath(-1, 2, emptyList())), actual.getResolvedQualifiers().get(0).getResolvePaths());
        // aa.ap
        assertEquals(asList(new ResolvePath(-1, 4, emptyList())), actual.getResolvedQualifiers().get(1).getResolvePaths());
        // x.s.art_id
        assertEquals(0, actual.getResolvedQualifiers().get(2).getLambdaId());
        // Since the targe ordinal is the same we should only have one resolve path here
        // even if we are working in a multiple alias context (unionall)
        assertEquals(asList(new ResolvePath(-1, 0, asList("art_id"))), actual.getResolvedQualifiers().get(2).getResolvePaths());

        // Combined column of different aliases
        e = e("unionall(aa, aa.ap).map(x -> x.art_id)");
        actual = SelectResolver.resolve(e, source);
        assertEquals(ofEntries(entry(articleAttribute, asSet("art_id")), entry(articlePrice, asSet("art_id"))), actual.getColumnsByAlias());
        // aa
        assertEquals(asList(new ResolvePath(-1, 2, emptyList())), actual.getResolvedQualifiers().get(0).getResolvePaths());
        // aa.ap
        assertEquals(asList(new ResolvePath(-1, 4, emptyList())), actual.getResolvedQualifiers().get(1).getResolvePaths());
        // x.art_id
        assertEquals(0, actual.getResolvedQualifiers().get(2).getLambdaId());
        // Since the targe ordinal is the same we should only have one resolve path here
        // even if we are working in a multiple alias context (unionall)
        assertEquals(asList(
                // Source is the sub query for the first one since we are starting from source
                new ResolvePath(2, 3, asList("art_id")),
                new ResolvePath(4, 4, asList("art_id"))),
                actual.getResolvedQualifiers().get(2).getResolvePaths());

        e = e("a.art_id = aa.art_id");
        actual = SelectResolver.resolve(e, source);
        assertEquals(ofEntries(entry(article, asSet("art_id")), entry(articleAttribute, asSet("art_id"))), actual.getColumnsByAlias());
        // a.art_id
        assertEquals(asList(new ResolvePath(-1, 1, asList("art_id"))), actual.getResolvedQualifiers().get(0).getResolvePaths());
        // aa.art_id
        assertEquals(asList(new ResolvePath(-1, 3, asList("art_id"))), actual.getResolvedQualifiers().get(1).getResolvePaths());

        e = e("a.art_id = aa.art_id and a.col1 = s.col2");
        actual = SelectResolver.resolve(e, source);
        assertEquals(ofEntries(entry(article, asSet("art_id", "col1")), entry(articleAttribute, asSet("art_id")), entry(source, asSet("col2"))), actual.getColumnsByAlias());
        // a.art_id
        assertEquals(asList(new ResolvePath(-1, 1, asList("art_id"))), actual.getResolvedQualifiers().get(0).getResolvePaths());
        // aa.art_id
        assertEquals(asList(new ResolvePath(-1, 3, asList("art_id"))), actual.getResolvedQualifiers().get(1).getResolvePaths());
        // a.col1
        assertEquals(asList(new ResolvePath(-1, 1, asList("col1"))), actual.getResolvedQualifiers().get(2).getResolvePaths());
        // s.col2
        assertEquals(asList(new ResolvePath(-1, 0, asList("col2"))), actual.getResolvedQualifiers().get(3).getResolvePaths());

        e = e("hash(1,2,2)");
        actual = SelectResolver.resolve(e, source);
        assertEquals(emptyMap(), actual.getColumnsByAlias());
        assertEquals(0, actual.getResolvedQualifiers().size());

        e = e("hash()");
        actual = SelectResolver.resolve(e, source);
        assertEquals(emptyMap(), actual.getColumnsByAlias());
        assertEquals(0, actual.getResolvedQualifiers().size());

        e = e("hash(art_id, aa.sku_id)");
        actual = SelectResolver.resolve(e, source);
        assertEquals(ofEntries(entry(source, asSet("art_id")), entry(articleAttribute, asSet("sku_id"))), actual.getColumnsByAlias());
        // art_id
        assertEquals(asList(new ResolvePath(-1, 0, asList("art_id"))), actual.getResolvedQualifiers().get(0).getResolvePaths());
        // aa.sku_id
        assertEquals(asList(new ResolvePath(-1, 3, asList("sku_id"))), actual.getResolvedQualifiers().get(1).getResolvePaths());

        // List refers to source
        // field Id is a column belonging to List which isn't
        // among the table hierarchy and hence unknown
        e = e("list.filter(l -> l.id > 0)");
        actual = SelectResolver.resolve(e, source);
        assertEquals(ofEntries(entry(source, asSet("list"))), actual.getColumnsByAlias());
        // list
        assertEquals(asList(new ResolvePath(-1, 0, asList("list"))), actual.getResolvedQualifiers().get(0).getResolvePaths());
        // l.id
        assertEquals(0, actual.getResolvedQualifiers().get(1).getLambdaId());
        assertEquals(asList(new ResolvePath(-1, -1, asList("id"))), actual.getResolvedQualifiers().get(1).getResolvePaths());

        e = e("aa.map(aa -> aa.ap.map(ap -> ap.price_sales + ap.price_org))");
        actual = SelectResolver.resolve(e, source);
        assertEquals(ofEntries(entry(articlePrice, asSet("price_sales", "price_org"))), actual.getColumnsByAlias());
        // aa
        assertEquals(asList(new ResolvePath(-1, 2, emptyList())), actual.getResolvedQualifiers().get(0).getResolvePaths());
        // aa.ap
        assertEquals(0, actual.getResolvedQualifiers().get(1).getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 4, emptyList())), actual.getResolvedQualifiers().get(1).getResolvePaths());
        // ap.price_sales
        assertEquals(1, actual.getResolvedQualifiers().get(2).getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 4, asList("price_sales"))), actual.getResolvedQualifiers().get(2).getResolvePaths());
        // ap.price_org
        assertEquals(1, actual.getResolvedQualifiers().get(3).getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 4, asList("price_org"))), actual.getResolvedQualifiers().get(3).getResolvePaths());

        e = e("aa.flatmap(aa -> aa.ap.map(ap -> ap.price_sales + ap.price_org))");
        actual = SelectResolver.resolve(e, source);
        assertEquals(ofEntries(entry(articlePrice, asSet("price_sales", "price_org"))), actual.getColumnsByAlias());
        // aa
        assertEquals(asList(new ResolvePath(-1, 2, emptyList())), actual.getResolvedQualifiers().get(0).getResolvePaths());
        // aa.ap
        assertEquals(0, actual.getResolvedQualifiers().get(1).getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 4, emptyList())), actual.getResolvedQualifiers().get(1).getResolvePaths());
        // ap.price_sales
        assertEquals(1, actual.getResolvedQualifiers().get(2).getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 4, asList("price_sales"))), actual.getResolvedQualifiers().get(2).getResolvePaths());
        // ap.price_org
        assertEquals(1, actual.getResolvedQualifiers().get(3).getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 4, asList("price_org"))), actual.getResolvedQualifiers().get(3).getResolvePaths());

        e = e("aa.map(aa -> aa.ap.map(ap -> ap.price_sales + ap.price_org + s.id))");
        actual = SelectResolver.resolve(e, source);
        assertEquals(ofEntries(entry(source, asSet("id")), entry(articlePrice, asSet("price_sales", "price_org"))), actual.getColumnsByAlias());
        // aa
        assertEquals(asList(new ResolvePath(-1, 2, emptyList())), actual.getResolvedQualifiers().get(0).getResolvePaths());
        // aa.ap
        assertEquals(0, actual.getResolvedQualifiers().get(1).getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 4, emptyList())), actual.getResolvedQualifiers().get(1).getResolvePaths());
        // ap.price_sales
        assertEquals(1, actual.getResolvedQualifiers().get(2).getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 4, asList("price_sales"))), actual.getResolvedQualifiers().get(2).getResolvePaths());
        // ap.price_org
        assertEquals(1, actual.getResolvedQualifiers().get(3).getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 4, asList("price_org"))), actual.getResolvedQualifiers().get(3).getResolvePaths());
        // s.id
        assertEquals(-1, actual.getResolvedQualifiers().get(4).getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 0, asList("id"))), actual.getResolvedQualifiers().get(4).getResolvePaths());

        e = e("aa.field.unknown");
        actual = SelectResolver.resolve(e, source);
        assertEquals(ofEntries(entry(articleAttribute, asSet("field"))), actual.getColumnsByAlias());
        // aa.field.unknown
        assertEquals(asList(new ResolvePath(-1, 3, asList("field", "unknown"))), actual.getResolvedQualifiers().get(0).getResolvePaths());

        e = e("aa.filter(aa -> aa.sku_id > 0)");
        actual = SelectResolver.resolve(e, source);
        assertEquals(ofEntries(entry(articleAttribute, asSet("sku_id"))), actual.getColumnsByAlias());
        // aa
        assertEquals(asList(new ResolvePath(-1, 2, emptyList())), actual.getResolvedQualifiers().get(0).getResolvePaths());
        // aa.sku_id
        assertEquals(asList(new ResolvePath(-1, 3, asList("sku_id"))), actual.getResolvedQualifiers().get(1).getResolvePaths());

        e = e("filter(aa.filter(aa -> aa.sku_id > 0), aa -> aa.attr1_id > 0)");
        actual = SelectResolver.resolve(e, source);
        assertEquals(ofEntries(entry(articleAttribute, asSet("sku_id", "attr1_id"))), actual.getColumnsByAlias());
        // aa
        assertEquals(asList(new ResolvePath(-1, 2, emptyList())), actual.getResolvedQualifiers().get(0).getResolvePaths());
        // aa.sku_id
        assertEquals(0, actual.getResolvedQualifiers().get(1).getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 3, asList("sku_id"))), actual.getResolvedQualifiers().get(1).getResolvePaths());
        // aa.attr1_id
        assertEquals(0, actual.getResolvedQualifiers().get(2).getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 3, asList("attr1_id"))), actual.getResolvedQualifiers().get(2).getResolvePaths());

        e = e("ap.sku_id = aa.sku_id");
        actual = SelectResolver.resolve(e, articlePrice);
        assertEquals(ofEntries(entry(articleAttribute, asSet("sku_id")), entry(articlePrice, asSet("sku_id"))), actual.getColumnsByAlias());
        // ap.sku_id
        assertEquals(asList(new ResolvePath(-1, 4, asList("sku_id"))), actual.getResolvedQualifiers().get(0).getResolvePaths());
        // aa.sku_id
        assertEquals(asList(new ResolvePath(-1, 3, asList("sku_id"))), actual.getResolvedQualifiers().get(1).getResolvePaths());

        // Traverse up in hierarchy and then down
        e = e("aBrand.articleBrandId = a.articleBrandId");
        actual = SelectResolver.resolve(e, articleBrand);
        assertEquals(ofEntries(entry(articleBrand, asSet("articleBrandId")), entry(article, asSet("articleBrandId"))), actual.getColumnsByAlias());
        // aBrand.articleBrandId
        assertEquals(asList(new ResolvePath(-1, 6, asList("articleBrandId"))), actual.getResolvedQualifiers().get(0).getResolvePaths());
        // a.articleBrandId
        assertEquals(asList(new ResolvePath(-1, 1, asList("articleBrandId"))), actual.getResolvedQualifiers().get(1).getResolvePaths());

        e = e("aa.flatMap(x -> x.ap).map(x -> x.price_sales)");
        actual = SelectResolver.resolve(e, source);
        assertEquals(ofEntries(entry(articlePrice, asSet("price_sales"))), actual.getColumnsByAlias());
        // aa
        assertEquals(asList(new ResolvePath(-1, 2, emptyList())), actual.getResolvedQualifiers().get(0).getResolvePaths());
        // x.ap
        assertEquals(0, actual.getResolvedQualifiers().get(1).getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 4, emptyList())), actual.getResolvedQualifiers().get(1).getResolvePaths());
        // x.price_sales
        assertEquals(0, actual.getResolvedQualifiers().get(2).getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 4, asList("price_sales"))), actual.getResolvedQualifiers().get(2).getResolvePaths());
    }

    private Expression e(String expression)
    {
        return parser.parseExpression(session.getCatalogRegistry(), expression);
    }
}
