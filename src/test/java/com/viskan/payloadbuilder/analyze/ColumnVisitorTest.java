package com.viskan.payloadbuilder.analyze;

import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.catalog.CatalogRegistry;
import com.viskan.payloadbuilder.parser.QueryParser;
import com.viskan.payloadbuilder.parser.tree.Expression;

import static com.viskan.payloadbuilder.analyze.ColumnsVisitor.getColumnsByAlias;
import static com.viskan.payloadbuilder.utils.MapUtils.entry;
import static com.viskan.payloadbuilder.utils.MapUtils.ofEntries;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;

import org.junit.Assert;
import org.junit.Test;

/** Test {@link ColumnsVisitor} */
public class ColumnVisitorTest extends Assert
{
    private final QueryParser parser = new QueryParser();
    private final CatalogRegistry catalogRegistry = new CatalogRegistry();

    @Test
    public void test()
    {
        TableAlias source = TableAlias.of(null, "source", "s");
        TableAlias article = TableAlias.of(source, "article", "a");
        TableAlias articleAttribute = TableAlias.of(source, "articleAttribute", "aa");
        TableAlias articlePrice = TableAlias.of(articleAttribute, "ariclePrice", "ap");
        TableAlias.of(articleAttribute, "aricleBalance", "ab");

        Expression e;
        e = parser.parseExpression(catalogRegistry, "a.col");
        assertEquals(ofEntries(entry(article, asList("col"))), getColumnsByAlias(source, e));

        e = parser.parseExpression(catalogRegistry, "col");
        assertEquals(ofEntries(entry(source, asList("col"))), getColumnsByAlias(source, e));

        e = parser.parseExpression(catalogRegistry, "a.col = aa.col");
        assertEquals(ofEntries(entry(article, asList("col")), entry(articleAttribute, asList("col"))), getColumnsByAlias(source, e));

        e = parser.parseExpression(catalogRegistry, "a.col = aa.col and a.col1 = s.col2");
        assertEquals(ofEntries(entry(article, asList("col", "col1")), entry(articleAttribute, asList("col")), entry(source, asList("col2"))), getColumnsByAlias(source, e));

        e = parser.parseExpression(catalogRegistry, "hash(1,2,2)");
        assertEquals(emptyMap(), getColumnsByAlias(source, e));

        e = parser.parseExpression(catalogRegistry, "hash()");
        assertEquals(emptyMap(), getColumnsByAlias(source, e));

        e = parser.parseExpression(catalogRegistry, "hash(art_id, aa.sku_id)");
        assertEquals(ofEntries(entry(source, asList("art_id")), entry(articleAttribute, asList("sku_id"))), getColumnsByAlias(source, e));

        // List refers to source
        // field Id is a column belonging to List which isn't
        // among the table hierarchy and hence unknown
        e = parser.parseExpression(catalogRegistry, "list.filter(l -> l.id > 0)");
        assertEquals(ofEntries(entry(source, asList("list"))), getColumnsByAlias(source, e));

        e = parser.parseExpression(catalogRegistry, "aa.filter(aa -> aa.sku_id > 0)");
        assertEquals(ofEntries(entry(articleAttribute, asList("sku_id"))), getColumnsByAlias(source, e));

        e = parser.parseExpression(catalogRegistry, "aa.ap.filter(ap -> ap.price_sales > 0)");
        assertEquals(ofEntries(entry(articlePrice, asList("price_sales"))), getColumnsByAlias(source, e));

        e = parser.parseExpression(catalogRegistry, "aa.map(aa -> aa.ap.map(ap -> ap.price_sales + ap.price_org))");
        assertEquals(ofEntries(entry(articlePrice, asList("price_sales", "price_org"))), getColumnsByAlias(source, e));

        e = parser.parseExpression(catalogRegistry, "aa.field.unknown");
        assertEquals(ofEntries(entry(articleAttribute, asList("field"))), getColumnsByAlias(source, e));

        e = parser.parseExpression(catalogRegistry, "aa.map(aa -> aa.ap.map(ap -> ap.price_sales + ap.price_org + s.id) + a.art_id)");
        assertEquals(ofEntries(entry(source, asList("id")), entry(article, asList("art_id")), entry(articlePrice, asList("price_sales", "price_org"))), getColumnsByAlias(source, e));
        
        e = parser.parseExpression(catalogRegistry, "filter(aa.filter(aa -> aa.sku_id > 0), aa -> aa.attr1_id > 0)");
        assertEquals(ofEntries(entry(articleAttribute, asList("sku_id", "attr1_id"))), getColumnsByAlias(source, e));

    }
}
