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
import static java.util.Collections.emptyMap;
import static org.kuse.payloadbuilder.core.utils.CollectionUtils.asSet;
import static org.kuse.payloadbuilder.core.utils.MapUtils.entry;
import static org.kuse.payloadbuilder.core.utils.MapUtils.ofEntries;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;
import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.catalog.CatalogRegistry;
import org.kuse.payloadbuilder.core.operator.TableAlias.TableAliasBuilder;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.parser.QueryParser;

/** Test {@link ColumnsVisitor} */
public class ColumnsVisitorTest extends Assert
{
    private final QueryParser parser = new QueryParser();
    private final QuerySession session = new QuerySession(new CatalogRegistry());

    @Test
    public void test()
    {
        TableAlias root = TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("ROOT"), "ROOT")
                .children(asList(
                        TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("source"), "s"),
                        TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("article"), "a"),
                        TableAliasBuilder.of(TableAlias.Type.SUBQUERY, QualifiedName.of("SubQuery"), "aa")
                                .children(asList(
                                        TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("articleAttribute"), "aa"),
                                        TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("articlePrice"), "ap"),
                                        TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("articleBalance"), "ab")

                                )),
                        TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("aricleBrand"), "aBrand")))
                .build();

        TableAlias source = root.getChildAliases().get(0);
        TableAlias article = root.getChildAliases().get(1);
        TableAlias subArticleAttribute = root.getChildAliases().get(2);
        TableAlias articleAttribute = subArticleAttribute.getChildAliases().get(0);
        TableAlias articlePrice = subArticleAttribute.getChildAliases().get(1);
        TableAlias articleBrand = root.getChildAliases().get(3);

        Set<TableAlias> actual;
        Expression e;
        Map<TableAlias, Set<String>> columnsByAlias = new HashMap<>();

        e = e("a.col");
        actual = ColumnsVisitor.getColumnsByAlias(session, columnsByAlias, source, e);
        assertEquals(asSet(source), actual);
        assertEquals(ofEntries(entry(article, asSet("col"))), columnsByAlias);

        columnsByAlias.clear();
        e = e("col");
        actual = ColumnsVisitor.getColumnsByAlias(session, columnsByAlias, source, e);
        assertEquals(asSet(source), actual);
        assertEquals(ofEntries(entry(source, asSet("col"))), columnsByAlias);

        columnsByAlias.clear();
        e = e("aa");
        actual = ColumnsVisitor.getColumnsByAlias(session, columnsByAlias, source, e);
        assertEquals(asSet(subArticleAttribute), actual);
        assertEquals(emptyMap(), columnsByAlias);

        columnsByAlias.clear();
        e = e("aa.ap");
        actual = ColumnsVisitor.getColumnsByAlias(session, columnsByAlias, source, e);
        assertEquals(asSet(articlePrice), actual);
        assertEquals(emptyMap(), columnsByAlias);
        
        columnsByAlias.clear();
        e = e("aa.map(x -> concat(x, ','))");
        actual = ColumnsVisitor.getColumnsByAlias(session, columnsByAlias, source, e);
        assertEquals(asSet(source), actual);
        assertEquals(emptyMap(), columnsByAlias);

        columnsByAlias.clear();
        e = e("aa.map(x -> x.ap)");
        actual = ColumnsVisitor.getColumnsByAlias(session, columnsByAlias, source, e);
        assertEquals(asSet(articlePrice), actual);
        assertEquals(emptyMap(), columnsByAlias);

        columnsByAlias.clear();
        e = e("aa.map(x -> x.ap.price_sales)");
        actual = ColumnsVisitor.getColumnsByAlias(session, columnsByAlias, source, e);
        assertEquals(asSet(source), actual);
        assertEquals(ofEntries(entry(articlePrice, asSet("price_sales"))), columnsByAlias);

        columnsByAlias.clear();
        e = e("aa.map(x -> x.ap)");
        actual = ColumnsVisitor.getColumnsByAlias(session, columnsByAlias, source, e);
        assertEquals(asSet(articlePrice), actual);
        assertEquals(emptyMap(), columnsByAlias);

        columnsByAlias.clear();
        e = e("concat(aa, aa.ap)");
        actual = ColumnsVisitor.getColumnsByAlias(session, columnsByAlias, source, e);
        assertEquals(asSet(subArticleAttribute, articlePrice), actual);
        assertEquals(emptyMap(), columnsByAlias);

        // Traverse down and then up to root again
        columnsByAlias.clear();
        e = e("concat(aa, aa.ap).map(x -> x.s.art_id)");
        actual = ColumnsVisitor.getColumnsByAlias(session, columnsByAlias, source, e);
        assertEquals(asSet(source), actual);
        assertEquals(ofEntries(entry(source, asSet("art_id"))), columnsByAlias);

        // Combined column of different aliases
        columnsByAlias.clear();
        e = e("concat(aa, aa.ap).map(x -> x.art_id)");
        actual = ColumnsVisitor.getColumnsByAlias(session, columnsByAlias, source, e);
        assertEquals(asSet(source), actual);
        assertEquals(ofEntries(entry(articleAttribute, asSet("art_id")), entry(articlePrice, asSet("art_id"))), columnsByAlias);

        columnsByAlias.clear();
        e = e("a.art_id = aa.art_id");
        actual = ColumnsVisitor.getColumnsByAlias(session, columnsByAlias, source, e);
        assertEquals(asSet(source), actual);
        assertEquals(ofEntries(entry(article, asSet("art_id")), entry(articleAttribute, asSet("art_id"))), columnsByAlias);

        columnsByAlias.clear();
        e = e("a.art_id = aa.art_id and a.col1 = s.col2");
        actual = ColumnsVisitor.getColumnsByAlias(session, columnsByAlias, source, e);
        assertEquals(asSet(source), actual);
        assertEquals(ofEntries(entry(article, asSet("art_id", "col1")), entry(articleAttribute, asSet("art_id")), entry(source, asSet("col2"))), columnsByAlias);

        columnsByAlias.clear();
        e = e("hash(1,2,2)");
        actual = ColumnsVisitor.getColumnsByAlias(session, columnsByAlias, source, e);
        assertEquals(asSet(source), actual);
        assertEquals(emptyMap(), columnsByAlias);

        columnsByAlias.clear();
        e = e("hash()");
        actual = ColumnsVisitor.getColumnsByAlias(session, columnsByAlias, source, e);
        assertEquals(asSet(source), actual);
        assertEquals(emptyMap(), columnsByAlias);

        columnsByAlias.clear();
        e = e("hash(art_id, aa.sku_id)");
        actual = ColumnsVisitor.getColumnsByAlias(session, columnsByAlias, source, e);
        assertEquals(asSet(source), actual);
        assertEquals(ofEntries(entry(source, asSet("art_id")), entry(articleAttribute, asSet("sku_id"))), columnsByAlias);

        columnsByAlias.clear();
        // List refers to source
        // field Id is a column belonging to List which isn't
        // among the table hierarchy and hence unknown
        e = e("list.filter(l -> l.id > 0)");
        actual = ColumnsVisitor.getColumnsByAlias(session, columnsByAlias, source, e);
        assertEquals(asSet(source), actual);
        assertEquals(ofEntries(entry(source, asSet("list"))), columnsByAlias);

        columnsByAlias.clear();
        e = e("aa.map(aa -> aa.ap.map(ap -> ap.price_sales + ap.price_org))");
        actual = ColumnsVisitor.getColumnsByAlias(session, columnsByAlias, source, e);
        assertEquals(asSet(source), actual);
        assertEquals(ofEntries(entry(articlePrice, asSet("price_sales", "price_org"))), columnsByAlias);

        columnsByAlias.clear();
        e = e("aa.flatmap(aa -> aa.ap.map(ap -> ap.price_sales + ap.price_org))");
        actual = ColumnsVisitor.getColumnsByAlias(session, columnsByAlias, source, e);
        assertEquals(asSet(source), actual);
        assertEquals(ofEntries(entry(articlePrice, asSet("price_sales", "price_org"))), columnsByAlias);

        columnsByAlias.clear();
        e = e("aa.map(aa -> aa.ap.map(ap -> ap.price_sales + ap.price_org + s.id))");
        actual = ColumnsVisitor.getColumnsByAlias(session, columnsByAlias, source, e);
        assertEquals(asSet(source), actual);
        assertEquals(ofEntries(entry(source, asSet("id")), entry(articlePrice, asSet("price_sales", "price_org"))), columnsByAlias);

        columnsByAlias.clear();
        e = e("aa.field.unknown");
        actual = ColumnsVisitor.getColumnsByAlias(session, columnsByAlias, source, e);
        assertEquals(asSet(source), actual);
        assertEquals(ofEntries(entry(articleAttribute, asSet("field"))), columnsByAlias);

        columnsByAlias.clear();
        e = e("aa.filter(aa -> aa.sku_id > 0)");
        actual = ColumnsVisitor.getColumnsByAlias(session, columnsByAlias, source, e);
        assertEquals(asSet(subArticleAttribute), actual);
        assertEquals(ofEntries(entry(articleAttribute, asSet("sku_id"))), columnsByAlias);

        columnsByAlias.clear();
        e = e("filter(aa.filter(aa -> aa.sku_id > 0), aa -> aa.attr1_id > 0)");
        actual = ColumnsVisitor.getColumnsByAlias(session, columnsByAlias, source, e);
        assertEquals(asSet(subArticleAttribute), actual);
        assertEquals(ofEntries(entry(articleAttribute, asSet("sku_id", "attr1_id"))), columnsByAlias);

        columnsByAlias.clear();
        e = e("ap.sku_id = aa.sku_id");
        actual = ColumnsVisitor.getColumnsByAlias(session, columnsByAlias, articlePrice, e);
        assertEquals(asSet(articlePrice), actual);
        assertEquals(ofEntries(entry(articleAttribute, asSet("sku_id")), entry(articlePrice, asSet("sku_id"))), columnsByAlias);

        // Traverse up in hierarchy and then down
        columnsByAlias.clear();
        e = e("aBrand.articleBrandId = a.articleBrandId");
        actual = ColumnsVisitor.getColumnsByAlias(session, columnsByAlias, articleBrand, e);
        assertEquals(asSet(articleBrand), actual);
        assertEquals(ofEntries(entry(articleBrand, asSet("articleBrandId")), entry(article, asSet("articleBrandId"))), columnsByAlias);

        columnsByAlias.clear();
        e = e("aa.flatMap(x -> x.ap).map(x -> x.price_sales)");
        actual = ColumnsVisitor.getColumnsByAlias(session, columnsByAlias, source, e);
        assertEquals(asSet(source), actual);
        assertEquals(ofEntries(entry(articlePrice, asSet("price_sales"))), columnsByAlias);
    }

    private Expression e(String expression)
    {
        return parser.parseExpression(expression);
    }
}
