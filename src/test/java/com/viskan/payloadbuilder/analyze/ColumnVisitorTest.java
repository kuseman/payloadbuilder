package com.viskan.payloadbuilder.analyze;

import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.catalog.CatalogRegistry;
import com.viskan.payloadbuilder.parser.QueryParser;
import com.viskan.payloadbuilder.parser.tree.Expression;

import static com.viskan.payloadbuilder.utils.CollectionUtils.asSet;
import static com.viskan.payloadbuilder.utils.MapUtils.entry;
import static com.viskan.payloadbuilder.utils.MapUtils.ofEntries;
import static java.util.Collections.emptyMap;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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

        Set<TableAlias> actual;
        Expression e;
        Map<TableAlias, Set<String>> columnsByAlias = new HashMap<>();
        
        e = parser.parseExpression(catalogRegistry, "a.col");
        actual = ColumnsVisitor.getColumnsByAlias(columnsByAlias, source, e);
        assertEquals(asSet(source), actual);
        assertEquals(ofEntries(entry(article, asSet("col"))), columnsByAlias);

        columnsByAlias.clear();
        e = parser.parseExpression(catalogRegistry, "col");
        actual = ColumnsVisitor.getColumnsByAlias(columnsByAlias, source, e);
        assertEquals(asSet(source), actual);
        assertEquals(ofEntries(entry(source, asSet("col"))), columnsByAlias);

        columnsByAlias.clear();
        e = parser.parseExpression(catalogRegistry, "aa");
        actual = ColumnsVisitor.getColumnsByAlias(columnsByAlias, source, e);
        assertEquals(asSet(articleAttribute), actual);
        assertEquals(emptyMap(), columnsByAlias);

        columnsByAlias.clear();
        e = parser.parseExpression(catalogRegistry, "aa.ap");
        actual = ColumnsVisitor.getColumnsByAlias(columnsByAlias, source, e);
        assertEquals(asSet(articlePrice), actual);
        assertEquals(emptyMap(), columnsByAlias);

        columnsByAlias.clear();
        e = parser.parseExpression(catalogRegistry, "aa.map(x -> x.ap)");
        actual = ColumnsVisitor.getColumnsByAlias(columnsByAlias, source, e);
        assertEquals(asSet(articlePrice), actual);
        assertEquals(emptyMap(), columnsByAlias);
        
        columnsByAlias.clear();
        e = parser.parseExpression(catalogRegistry, "aa.map(x -> x.ap.price_sales)");
        actual = ColumnsVisitor.getColumnsByAlias(columnsByAlias, source, e);
        assertEquals(asSet(source), actual);
        assertEquals(ofEntries(entry(articlePrice, asSet("price_sales"))), columnsByAlias);

        columnsByAlias.clear();
        e = parser.parseExpression(catalogRegistry, "aa.map(x -> x.ap)");
        actual = ColumnsVisitor.getColumnsByAlias(columnsByAlias, source, e);
        assertEquals(asSet(articlePrice), actual);
        assertEquals(emptyMap(), columnsByAlias);

        columnsByAlias.clear();
        e = parser.parseExpression(catalogRegistry, "concat(aa, aa.ap)");
        actual = ColumnsVisitor.getColumnsByAlias(columnsByAlias, source, e);
        assertEquals(asSet(articleAttribute, articlePrice), actual);
        assertEquals(emptyMap(), columnsByAlias);

        // Traverse down and then up to root again
        columnsByAlias.clear();
        e = parser.parseExpression(catalogRegistry, "concat(aa, aa.ap).map(x -> x.s.art_id)");
        actual = ColumnsVisitor.getColumnsByAlias(columnsByAlias, source, e);
        assertEquals(asSet(source), actual);
        assertEquals(ofEntries(entry(source, asSet("art_id"))), columnsByAlias);

        // Combined column of different aliases
        columnsByAlias.clear();
        e = parser.parseExpression(catalogRegistry, "concat(aa, aa.ap).map(x -> x.art_id)");
        actual = ColumnsVisitor.getColumnsByAlias(columnsByAlias, source, e);
        assertEquals(asSet(source), actual);
        assertEquals(ofEntries(entry(articleAttribute, asSet("art_id")), entry(articlePrice, asSet("art_id"))), columnsByAlias);

        columnsByAlias.clear();
        e = parser.parseExpression(catalogRegistry, "a.art_id = aa.art_id");
        actual = ColumnsVisitor.getColumnsByAlias(columnsByAlias, source, e);
        assertEquals(asSet(source), actual);
        assertEquals(ofEntries(entry(article, asSet("art_id")), entry(articleAttribute, asSet("art_id"))), columnsByAlias);

        columnsByAlias.clear();
        e = parser.parseExpression(catalogRegistry, "a.art_id = aa.art_id and a.col1 = s.col2");
        actual = ColumnsVisitor.getColumnsByAlias(columnsByAlias, source, e);
        assertEquals(asSet(source), actual);
        assertEquals(ofEntries(entry(article, asSet("art_id", "col1")), entry(articleAttribute, asSet("art_id")), entry(source, asSet("col2"))), columnsByAlias);

        columnsByAlias.clear();
        e = parser.parseExpression(catalogRegistry, "hash(1,2,2)");
        actual = ColumnsVisitor.getColumnsByAlias(columnsByAlias, source, e);
        assertEquals(asSet(source), actual);
        assertEquals(emptyMap(), columnsByAlias);

        columnsByAlias.clear();
        e = parser.parseExpression(catalogRegistry, "hash()");
        actual = ColumnsVisitor.getColumnsByAlias(columnsByAlias, source, e);
        assertEquals(asSet(source), actual);
        assertEquals(emptyMap(), columnsByAlias);

        columnsByAlias.clear();
        e = parser.parseExpression(catalogRegistry, "hash(art_id, aa.sku_id)");
        actual = ColumnsVisitor.getColumnsByAlias(columnsByAlias, source, e);
        assertEquals(asSet(source), actual);
        assertEquals(ofEntries(entry(source, asSet("art_id")), entry(articleAttribute, asSet("sku_id"))), columnsByAlias);

        columnsByAlias.clear();
        // List refers to source
        // field Id is a column belonging to List which isn't
        // among the table hierarchy and hence unknown
        e = parser.parseExpression(catalogRegistry, "list.filter(l -> l.id > 0)");
        actual = ColumnsVisitor.getColumnsByAlias(columnsByAlias, source, e);
        assertEquals(asSet(source), actual);
        assertEquals(ofEntries(entry(source, asSet("list"))), columnsByAlias);

        columnsByAlias.clear();
        e = parser.parseExpression(catalogRegistry, "aa.map(aa -> aa.ap.map(ap -> ap.price_sales + ap.price_org))");
        actual = ColumnsVisitor.getColumnsByAlias(columnsByAlias, source, e);
        assertEquals(asSet(source), actual);
        assertEquals(ofEntries(entry(articlePrice, asSet("price_sales", "price_org"))), columnsByAlias);

        columnsByAlias.clear();
        e = parser.parseExpression(catalogRegistry, "aa.flatmap(aa -> aa.ap.map(ap -> ap.price_sales + ap.price_org))");
        actual = ColumnsVisitor.getColumnsByAlias(columnsByAlias, source, e);
        assertEquals(asSet(source), actual);
        assertEquals(ofEntries(entry(articlePrice, asSet("price_sales", "price_org"))), columnsByAlias);

        columnsByAlias.clear();
        e = parser.parseExpression(catalogRegistry, "aa.map(aa -> aa.ap.map(ap -> ap.price_sales + ap.price_org + s.id))");
        actual = ColumnsVisitor.getColumnsByAlias(columnsByAlias, source, e);
        assertEquals(asSet(source), actual);
        assertEquals(ofEntries(entry(source, asSet("id")), entry(articlePrice, asSet("price_sales", "price_org"))), columnsByAlias);

        columnsByAlias.clear();
        e = parser.parseExpression(catalogRegistry, "aa.field.unknown");
        actual = ColumnsVisitor.getColumnsByAlias(columnsByAlias, source, e);
        assertEquals(asSet(source), actual);
        assertEquals(ofEntries(entry(articleAttribute, asSet("field"))), columnsByAlias);
        
        columnsByAlias.clear();
        e = parser.parseExpression(catalogRegistry, "aa.filter(aa -> aa.sku_id > 0)");
        actual = ColumnsVisitor.getColumnsByAlias(columnsByAlias, source, e);
        assertEquals(asSet(articleAttribute), actual);
        assertEquals(ofEntries(entry(articleAttribute, asSet("sku_id"))), columnsByAlias);

        columnsByAlias.clear();
        e = parser.parseExpression(catalogRegistry, "filter(aa.filter(aa -> aa.sku_id > 0), aa -> aa.attr1_id > 0)");
        actual = ColumnsVisitor.getColumnsByAlias(columnsByAlias, source, e);
        assertEquals(asSet(articleAttribute), actual);
        assertEquals(ofEntries(entry(articleAttribute, asSet("sku_id", "attr1_id"))), columnsByAlias);

        columnsByAlias.clear();
        e = parser.parseExpression(catalogRegistry, "ap.sku_id = aa.sku_id");
        actual = ColumnsVisitor.getColumnsByAlias(columnsByAlias, articlePrice, e);
        assertEquals(asSet(articlePrice), actual);
        assertEquals(ofEntries(entry(articleAttribute, asSet("sku_id")), entry(articlePrice, asSet("sku_id"))), columnsByAlias);
    }
}
