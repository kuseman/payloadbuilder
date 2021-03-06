package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

import org.junit.Assert;
import org.junit.Test;
import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.catalog.CatalogRegistry;
import org.kuse.payloadbuilder.core.operator.SelectResolver.Context;
import org.kuse.payloadbuilder.core.operator.TableAlias.TableAliasBuilder;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.ParseException;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.parser.QualifiedReferenceExpression.ResolvePath;
import org.kuse.payloadbuilder.core.parser.QueryParser;
import org.kuse.payloadbuilder.core.parser.QueryStatement;
import org.kuse.payloadbuilder.core.parser.Select;
import org.kuse.payloadbuilder.core.parser.SelectStatement;

/** Test {@link SelectResolver} */
public class SelectResolverTest extends Assert
{
    private final QuerySession session = new QuerySession(new CatalogRegistry());
    private final QueryParser parser = new QueryParser();

    @Test
    public void test_unknowm_column()
    {
        Select s = s("select x.dummy from range(1, 10) x");
        assertResolveFail(ParseException.class, "Unknown column: 'dummy' in table source: 'range x', expected one of: [Value]", s);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void test_column_resolving_with_case_insensitiveness()
    {
        Select s = s("select x.vAlUe from range(1, 10) x");
        SelectResolver.Context actual = SelectResolver.resolveForTest(session, s);
        // vAlUe
        assertEquals(asList(new ResolvePath(-1, 0, emptyList(), 0)), asList(actual.getResolvedQualifiers().get(0).getResolvePaths()));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void test_selects()
    {
        SelectResolver.Context actual;
        Select s;

        s = s("select col1,"
            + "(select col2, col3 where col5 != col6 for object) obj "
            + "from table");

        actual = SelectResolver.resolveForTest(session, s);
        // col1
        assertEquals(asList(new ResolvePath(-1, 0, asList("col1"), -1)), asList(actual.getResolvedQualifiers().get(0).getResolvePaths()));
        // col5
        assertEquals(asList(new ResolvePath(-1, 0, asList("col5"), -1)), asList(actual.getResolvedQualifiers().get(1).getResolvePaths()));
        // col6
        assertEquals(asList(new ResolvePath(-1, 0, asList("col6"), -1)), asList(actual.getResolvedQualifiers().get(2).getResolvePaths()));
        // col2
        assertEquals(asList(new ResolvePath(-1, 0, asList("col2"), -1)), asList(actual.getResolvedQualifiers().get(3).getResolvePaths()));
        // col3
        assertEquals(asList(new ResolvePath(-1, 0, asList("col3"), -1)), asList(actual.getResolvedQualifiers().get(4).getResolvePaths()));

        s = s("select col1,"
            + "(select col2, col3 where col5 != col6 and b.col7 != 'value' for object) obj "
            + "from tableA a "
            + "inner join tableB b "
            + "  on b.id = a.id");

        actual = SelectResolver.resolveForTest(session, s);
        s = s("select col1, "
            + "(select col2, col3 where col5 != col6 and b.col7 != 'value' for object) object, "
            // Acess to subquery column AND outer scope column from tableB
            + "(select Value, b.col10, b.id from range(1,10) for array) array "
            + "from tableA a "
            + "inner join tableB b "
            + "  on b.id = a.id");

        actual = SelectResolver.resolveForTest(session, s);
        assertEquals(11, actual.getResolvedQualifiers().size());
        // b.id
        assertEquals(asList(new ResolvePath(-1, 1, asList("id"), -1)), asList(actual.getResolvedQualifiers().get(0).getResolvePaths()));
        // a.id
        assertEquals(asList(new ResolvePath(-1, 0, asList("id"), -1)), asList(actual.getResolvedQualifiers().get(1).getResolvePaths()));
        // col1
        assertEquals(asList(new ResolvePath(-1, 0, asList("col1"), -1)), asList(actual.getResolvedQualifiers().get(2).getResolvePaths()));
        // col5
        assertEquals(asList(new ResolvePath(-1, 0, asList("col5"), -1)), asList(actual.getResolvedQualifiers().get(3).getResolvePaths()));
        // col6
        assertEquals(asList(new ResolvePath(-1, 0, asList("col6"), -1)), asList(actual.getResolvedQualifiers().get(4).getResolvePaths()));
        // col7
        assertEquals(asList(new ResolvePath(-1, 1, asList("col7"), -1)), asList(actual.getResolvedQualifiers().get(5).getResolvePaths()));
        // col2
        assertEquals(asList(new ResolvePath(-1, 0, asList("col2"), -1)), asList(actual.getResolvedQualifiers().get(6).getResolvePaths()));
        // col3
        assertEquals(asList(new ResolvePath(-1, 0, asList("col3"), -1)), asList(actual.getResolvedQualifiers().get(7).getResolvePaths()));
        // Value
        assertEquals(asList(new ResolvePath(-1, 4, emptyList(), 0)), asList(actual.getResolvedQualifiers().get(8).getResolvePaths()));
        // b.col10
        assertEquals(asList(new ResolvePath(-1, 1, asList("col10"), -1)), asList(actual.getResolvedQualifiers().get(9).getResolvePaths()));
        // b.id
        assertEquals(asList(new ResolvePath(-1, 1, asList("id"), -1)), asList(actual.getResolvedQualifiers().get(10).getResolvePaths()));

        s = s("select col, x.col2.key "
            + "from "
            + "( "
            + "  select 'value' col, "
            + "        (select 123 key for object) col2"
            + ") x");
        actual = SelectResolver.resolveForTest(session, s);

        s = s("select * " +
            "from range(1, 2) v " +
            "inner join " +
            "(" +
            "  select " +
            "  (" +
            "    select Value key " +
            "    for object " +
            "  ) map " +
            "  from range(1, 2) " +
            ") x" +
            "  on  x.map.key = v.Value");
        actual = SelectResolver.resolveForTest(session, s);
        assertEquals(3, actual.getResolvedQualifiers().size());
        // Value
        assertEquals(asList(new ResolvePath(-1, 2, emptyList(), 0)), asList(actual.getResolvedQualifiers().get(0).getResolvePaths()));
        // x.map.key
        assertEquals(asList(new ResolvePath(-1, 2, asList("map", "key"), 0)), asList(actual.getResolvedQualifiers().get(1).getResolvePaths()));
        // v.Value
        assertEquals(asList(new ResolvePath(-1, 0, emptyList(), 0)), asList(actual.getResolvedQualifiers().get(2).getResolvePaths()));

        // Nested TVF's
        s = s("select Value, " +
            "(" +
            "  select Value " +
            "  ," +
            "  (" +
            "    select Value " +
            "    from open_rows(r1) " +
            "    for object " +
            "  )  obj " +
            "  from open_rows(x) " +
            "  for object " +
            ") " +
            "from range(1, 2) v " +
            "inner join " +
            "(" +
            "  select * " +
            "  from range(1, 2) r " +
            "  inner join range(1, 2) r1 " +
            "    on r1.Value = r.Value " +
            ") x" +
            "  on  x.Value = v.Value");
        actual = SelectResolver.resolveForTest(session, s);
        assertEquals(9, actual.getResolvedQualifiers().size());
        // r1.Value
        assertEquals(asList(new ResolvePath(-1, 3, emptyList(), 0)), asList(actual.getResolvedQualifiers().get(0).getResolvePaths()));
        // r.Value
        assertEquals(asList(new ResolvePath(-1, 2, emptyList(), 0)), asList(actual.getResolvedQualifiers().get(1).getResolvePaths()));
        // x.Value
        assertEquals(asList(new ResolvePath(-1, 2, emptyList(), 0)), asList(actual.getResolvedQualifiers().get(2).getResolvePaths()));
        // v.Value
        assertEquals(asList(new ResolvePath(-1, 0, emptyList(), 0)), asList(actual.getResolvedQualifiers().get(3).getResolvePaths()));
        // Value (top select)
        assertEquals(asList(new ResolvePath(-1, 0, emptyList(), 0)), asList(actual.getResolvedQualifiers().get(4).getResolvePaths()));
        // x (open_rows(x))
        assertEquals(asList(new ResolvePath(-1, 1, emptyList(), -1)), asList(actual.getResolvedQualifiers().get(5).getResolvePaths()));
        // Value (first sub query select => x.Value)
        assertEquals(asList(new ResolvePath(-1, 2, emptyList(), 0)), asList(actual.getResolvedQualifiers().get(6).getResolvePaths()));
        // r1 (open_rows(r1)
        assertEquals(asList(new ResolvePath(-1, 3, emptyList(), -1)), asList(actual.getResolvedQualifiers().get(7).getResolvePaths()));
        // Value (second sub query select -> r1.Value)
        assertEquals(asList(new ResolvePath(-1, 3, emptyList(), 0)), asList(actual.getResolvedQualifiers().get(8).getResolvePaths()));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void test_temporary_tables()
    {
        String query = ""
            + "select aa.col5, bb.col6 "
            + "into #temp1 "
            + "from tableAA aa "
            + "inner join tableBB bb "
            + "  on bb.col = aa.id "
            + ""
            + "select a.col, b.col2 "
            + "into #temp "
            + "from tableA a "
            + "inner join tableB b "
            + "  on b.id = a.id "
            + "inner join "
            + "("
            + "  select * "
            + "  from tableC c "
            + "  inner join tableD d "
            + "    on d.id = c.id "
            + "  inner join #temp1 t "
            + "    on t.id = d.id "
            + ") x"
            + "  on x.id = a.id "
            + " "
            + "select col, t.b, t.b.col3, t.x.d, t.x.t.bb, t.x.t.aa.col5, t.x.t.col5 "
            + "from #temp t ";

        QueryStatement stm = parser.parseQuery(session.getCatalogRegistry(), query);

        Select select = ((SelectStatement) stm.getStatements().get(0)).getSelect();
        TableAlias tempTableAlias = select.getFrom().getTableSource().getTableAlias();
        TemporaryTable table = new TemporaryTable(select.getInto().getTable(), tempTableAlias, new String[] {"col5", "col6"}, emptyList());
        session.setTemporaryTable(table);
        select = ((SelectStatement) stm.getStatements().get(1)).getSelect();
        tempTableAlias = select.getFrom().getTableSource().getTableAlias();
        table = new TemporaryTable(select.getInto().getTable(), tempTableAlias, new String[] {"col", "col2"}, emptyList());
        session.setTemporaryTable(table);

        Context actual = SelectResolver.resolveForTest(session, ((SelectStatement) stm.getStatements().get(2)).getSelect());
        // col
        assertEquals(asList(new ResolvePath(-1, -1, emptyList(), 0)), asList(actual.getResolvedQualifiers().get(0).getResolvePaths()));
        // t.b
        assertEquals(asList(new ResolvePath(-1, -1, emptyList(), -1, new int[] {1})), asList(actual.getResolvedQualifiers().get(1).getResolvePaths()));
        // t.b.col3
        assertEquals(asList(new ResolvePath(-1, -1, asList("col3"), -1, new int[] {1})), asList(actual.getResolvedQualifiers().get(2).getResolvePaths()));
        // t.x.d => resolve into temp table sub query
        assertEquals(asList(new ResolvePath(-1, -1, emptyList(), -1, new int[] {4})), asList(actual.getResolvedQualifiers().get(3).getResolvePaths()));
        // t.x.t.bb => resolve into temp table sub query and then into another temp table's alias
        assertEquals(asList(new ResolvePath(-1, -1, emptyList(), -1, new int[] {5, 1})), asList(actual.getResolvedQualifiers().get(4).getResolvePaths()));
        // t.x.t.aa.col => resolve into temp table sub query and then into another temp table's alias with column
        assertEquals(asList(new ResolvePath(-1, -1, asList("col5"), -1, new int[] {5, 0})), asList(actual.getResolvedQualifiers().get(5).getResolvePaths()));
        // t.x.t.col5
        assertEquals(asList(new ResolvePath(-1, -1, emptyList(), 0, new int[] {5})), asList(actual.getResolvedQualifiers().get(6).getResolvePaths()));
    }

    @Test
    public void test_temporary_tables_fails()
    {
        String query = ""
            + "select col, col2 "
            + "into #temp "
            + "from table "
            + " "
            + "select col3 from #temp ";

        QueryStatement stm = parser.parseQuery(session.getCatalogRegistry(), query);

        Select select = ((SelectStatement) stm.getStatements().get(0)).getSelect();
        TableAlias tempTableAlias = select.getFrom().getTableSource().getTableAlias();
        TemporaryTable table = new TemporaryTable(select.getInto().getTable(), tempTableAlias, new String[] {"col1", "col2"}, emptyList());
        session.setTemporaryTable(table);

        assertResolveFail(ParseException.class, "Unkown column 'col3' in temporary table #temp", ((SelectStatement) stm.getStatements().get(1)).getSelect());
    }

    @Test
    public void test_alias_policy()
    {
        s("select art_id from article");
        s("select art_id from article where a.id = 10");
        assertSelectFail(ParseException.class, "Invalid table source reference 'b'", "select art_id from article a where b.art_id > 10");
        assertSelectFail(ParseException.class, "Alias is mandatory", "select * from tableA inner join tableB b on b.art_id = art_id");
        assertSelectFail(ParseException.class, "Alias is mandatory", "select * from tableA a inner join tableB on b.art_id = art_id");
        assertSelectFail(ParseException.class, "Invalid table source reference 'q'", "select art_id from article a inner join (select * from tableB b where id= 1 and q.id = 10) b on b.id = a.id");
        // Test reference to a parent is ok
        s("select art_id from article a inner join (select * from tableB b where id= 1 and a.id = 10) b on b.id = a.id");
        // c is not allowed since it's defined later that current context alias
        assertSelectFail(ParseException.class, "Invalid table source reference 'c'",
                "select art_id from article a inner join (select * from tableB b where id= 1 and c.id = 10) b on b.id = a.id inner join tableC c on c.id = b.id");
    }

    @Test
    public void test_selects_fail()
    {
        assertSelectFail(ParseException.class, "Unkown reference col1", "select col1,"
            + "(select col2, col3 where col5 != col6 for object) obj ");

        // Make sure that expression sub query table aliases isn't accessible from outer scope
        assertSelectFail(ParseException.class, "Invalid table source reference 'b'", ""
            + "select col "
            + ", (select aCol1, aCol2, a.col3 from tableB b for object) obj "
            + "from tableA a "
            + "where b.col10 > 10");

        assertSelectFail(ParseException.class, "Unkown reference col1", "select col1");
        assertSelectFail(ParseException.class, "No alias found with name: a", "select a.* from table");
    }

    @SuppressWarnings("deprecation")
    @Test
    public void test_expressions()
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
        TableAlias subArticleAttribute = root.getChildAliases().get(2);
        TableAlias articlePrice = subArticleAttribute.getChildAliases().get(0).getChildAliases().get(1);
        TableAlias articleBrand = root.getChildAliases().get(3);

        SelectResolver.Context actual;
        Expression e;

        e = e("a.col");
        actual = SelectResolver.resolveTorTest(e, source);
        assertEquals(asList(new ResolvePath(-1, 1, asList("col"), -1)), asList(actual.getResolvedQualifiers().get(0).getResolvePaths()));

        e = e("col");
        actual = SelectResolver.resolveTorTest(e, source);
        assertEquals(asList(new ResolvePath(-1, 0, asList("col"), -1)), asList(actual.getResolvedQualifiers().get(0).getResolvePaths()));

        e = e("aa");
        actual = SelectResolver.resolveTorTest(e, source);
        assertEquals(asList(new ResolvePath(-1, 2, emptyList(), -1)), asList(actual.getResolvedQualifiers().get(0).getResolvePaths()));

        e = e("aa.ap");
        actual = SelectResolver.resolveTorTest(e, source);
        assertEquals(asList(new ResolvePath(-1, 4, emptyList(), -1)), asList(actual.getResolvedQualifiers().get(0).getResolvePaths()));

        e = e("aa.map(x -> x)");
        actual = SelectResolver.resolveTorTest(e, source);
        // aa
        assertEquals(asList(new ResolvePath(-1, 2, emptyList(), -1)), asList(actual.getResolvedQualifiers().get(0).getResolvePaths()));
        // x
        assertEquals(0, actual.getResolvedQualifiers().get(1).getLambdaId());
        assertEquals(asList(new ResolvePath(-1, -1, emptyList(), -1)), asList(actual.getResolvedQualifiers().get(1).getResolvePaths()));

        e = e("aa.map(x -> concat(x, ','))");
        actual = SelectResolver.resolveTorTest(e, source);
        // aa
        assertEquals(asList(new ResolvePath(-1, 2, emptyList(), -1)), asList(actual.getResolvedQualifiers().get(0).getResolvePaths()));
        // x
        assertEquals(0, actual.getResolvedQualifiers().get(1).getLambdaId());
        assertEquals(asList(new ResolvePath(-1, -1, emptyList(), -1)), asList(actual.getResolvedQualifiers().get(1).getResolvePaths()));

        e = e("aa.map(x -> x.ap)");
        actual = SelectResolver.resolveTorTest(e, source);
        // aa
        assertEquals(asList(new ResolvePath(-1, 2, emptyList(), -1)), asList(actual.getResolvedQualifiers().get(0).getResolvePaths()));
        // x.ap
        assertEquals(0, actual.getResolvedQualifiers().get(1).getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 4, emptyList(), -1)), asList(actual.getResolvedQualifiers().get(1).getResolvePaths()));

        e = e("aa.map(x -> x.ap.price_sales)");
        actual = SelectResolver.resolveTorTest(e, source);
        // aa
        assertEquals(asList(new ResolvePath(-1, 2, emptyList(), -1)), asList(actual.getResolvedQualifiers().get(0).getResolvePaths()));
        // x.ap.price_sales
        assertEquals(0, actual.getResolvedQualifiers().get(1).getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 4, asList("price_sales"), -1)), asList(actual.getResolvedQualifiers().get(1).getResolvePaths()));

        e = e("unionall(aa, aa.ap)");
        actual = SelectResolver.resolveTorTest(e, source);
        // aa
        assertEquals(asList(new ResolvePath(-1, 2, emptyList(), -1)), asList(actual.getResolvedQualifiers().get(0).getResolvePaths()));
        // aa.ap
        assertEquals(asList(new ResolvePath(-1, 4, emptyList(), -1)), asList(actual.getResolvedQualifiers().get(1).getResolvePaths()));

        // Traverse down and then up to root again
        e = e("unionall(aa, aa.ap).map(x -> x.s.art_id)");
        actual = SelectResolver.resolveTorTest(e, source);
        // aa
        assertEquals(asList(new ResolvePath(-1, 2, emptyList(), -1)), asList(actual.getResolvedQualifiers().get(0).getResolvePaths()));
        // aa.ap
        assertEquals(asList(new ResolvePath(-1, 4, emptyList(), -1)), asList(actual.getResolvedQualifiers().get(1).getResolvePaths()));
        // x.s.art_id
        assertEquals(0, actual.getResolvedQualifiers().get(2).getLambdaId());
        // Since the targe ordinal is the same we should only have one resolve path here
        // even if we are working in a multiple alias context (unionall)
        assertEquals(asList(new ResolvePath(-1, 0, asList("art_id"), -1)), asList(actual.getResolvedQualifiers().get(2).getResolvePaths()));

        // Combined column of different aliases
        e = e("unionall(aa, aa.ap).map(x -> x.art_id)");
        actual = SelectResolver.resolveTorTest(e, source);
        // aa
        assertEquals(asList(new ResolvePath(-1, 2, emptyList(), -1)), asList(actual.getResolvedQualifiers().get(0).getResolvePaths()));
        // aa.ap
        assertEquals(asList(new ResolvePath(-1, 4, emptyList(), -1)), asList(actual.getResolvedQualifiers().get(1).getResolvePaths()));
        // x.art_id
        assertEquals(0, actual.getResolvedQualifiers().get(2).getLambdaId());
        assertEquals(asList(
                // Source is the sub query for the first one since we are starting from source
                new ResolvePath(2, 3, asList("art_id"), -1),
                new ResolvePath(4, 4, asList("art_id"), -1)),
                asList(actual.getResolvedQualifiers().get(2).getResolvePaths()));

        e = e("a.art_id = aa.art_id");
        actual = SelectResolver.resolveTorTest(e, source);
        // a.art_id
        assertEquals(asList(new ResolvePath(-1, 1, asList("art_id"), -1)), asList(actual.getResolvedQualifiers().get(0).getResolvePaths()));
        // aa.art_id
        assertEquals(asList(new ResolvePath(-1, 3, asList("art_id"), -1)), asList(actual.getResolvedQualifiers().get(1).getResolvePaths()));

        e = e("a.art_id = aa.art_id and a.col1 = s.col2");
        actual = SelectResolver.resolveTorTest(e, source);
        // a.art_id
        assertEquals(asList(new ResolvePath(-1, 1, asList("art_id"), -1)), asList(actual.getResolvedQualifiers().get(0).getResolvePaths()));
        // aa.art_id
        assertEquals(asList(new ResolvePath(-1, 3, asList("art_id"), -1)), asList(actual.getResolvedQualifiers().get(1).getResolvePaths()));
        // a.col1
        assertEquals(asList(new ResolvePath(-1, 1, asList("col1"), -1)), asList(actual.getResolvedQualifiers().get(2).getResolvePaths()));
        // s.col2
        assertEquals(asList(new ResolvePath(-1, 0, asList("col2"), -1)), asList(actual.getResolvedQualifiers().get(3).getResolvePaths()));

        e = e("hash(1,2,2)");
        actual = SelectResolver.resolveTorTest(e, source);
        assertEquals(0, actual.getResolvedQualifiers().size());

        e = e("hash()");
        actual = SelectResolver.resolveTorTest(e, source);
        assertEquals(0, actual.getResolvedQualifiers().size());

        e = e("hash(art_id, aa.sku_id)");
        actual = SelectResolver.resolveTorTest(e, source);
        // art_id
        assertEquals(asList(new ResolvePath(-1, 0, asList("art_id"), -1)), asList(actual.getResolvedQualifiers().get(0).getResolvePaths()));
        // aa.sku_id
        assertEquals(asList(new ResolvePath(-1, 3, asList("sku_id"), -1)), asList(actual.getResolvedQualifiers().get(1).getResolvePaths()));

        // List refers to source
        // field Id is a column belonging to List which isn't
        // among the table hierarchy and hence unknown
        e = e("list.filter(l -> l.id > 0)");
        actual = SelectResolver.resolveTorTest(e, source);
        // list
        assertEquals(asList(new ResolvePath(-1, 0, asList("list"), -1)), asList(actual.getResolvedQualifiers().get(0).getResolvePaths()));
        // l.id
        assertEquals(0, actual.getResolvedQualifiers().get(1).getLambdaId());
        assertEquals(asList(new ResolvePath(-1, -1, asList("id"), -1)), asList(actual.getResolvedQualifiers().get(1).getResolvePaths()));

        e = e("aa.map(aa -> aa.ap.map(ap -> ap.price_sales + ap.price_org))");
        actual = SelectResolver.resolveTorTest(e, source);
        // aa
        assertEquals(asList(new ResolvePath(-1, 2, emptyList(), -1)), asList(actual.getResolvedQualifiers().get(0).getResolvePaths()));
        // aa.ap
        assertEquals(0, actual.getResolvedQualifiers().get(1).getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 4, emptyList(), -1)), asList(actual.getResolvedQualifiers().get(1).getResolvePaths()));
        // ap.price_sales
        assertEquals(1, actual.getResolvedQualifiers().get(2).getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 4, asList("price_sales"), -1)), asList(actual.getResolvedQualifiers().get(2).getResolvePaths()));
        // ap.price_org
        assertEquals(1, actual.getResolvedQualifiers().get(3).getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 4, asList("price_org"), -1)), asList(actual.getResolvedQualifiers().get(3).getResolvePaths()));

        e = e("aa.flatmap(aa -> aa.ap.map(ap -> ap.price_sales + ap.price_org))");
        actual = SelectResolver.resolveTorTest(e, source);
        // aa
        assertEquals(asList(new ResolvePath(-1, 2, emptyList(), -1)), asList(actual.getResolvedQualifiers().get(0).getResolvePaths()));
        // aa.ap
        assertEquals(0, actual.getResolvedQualifiers().get(1).getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 4, emptyList(), -1)), asList(actual.getResolvedQualifiers().get(1).getResolvePaths()));
        // ap.price_sales
        assertEquals(1, actual.getResolvedQualifiers().get(2).getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 4, asList("price_sales"), -1)), asList(actual.getResolvedQualifiers().get(2).getResolvePaths()));
        // ap.price_org
        assertEquals(1, actual.getResolvedQualifiers().get(3).getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 4, asList("price_org"), -1)), asList(actual.getResolvedQualifiers().get(3).getResolvePaths()));

        e = e("aa.map(aa -> aa.ap.map(ap -> ap.price_sales + ap.price_org + s.id))");
        actual = SelectResolver.resolveTorTest(e, source);
        // aa
        assertEquals(asList(new ResolvePath(-1, 2, emptyList(), -1)), asList(actual.getResolvedQualifiers().get(0).getResolvePaths()));
        // aa.ap
        assertEquals(0, actual.getResolvedQualifiers().get(1).getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 4, emptyList(), -1)), asList(actual.getResolvedQualifiers().get(1).getResolvePaths()));
        // ap.price_sales
        assertEquals(1, actual.getResolvedQualifiers().get(2).getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 4, asList("price_sales"), -1)), asList(actual.getResolvedQualifiers().get(2).getResolvePaths()));
        // ap.price_org
        assertEquals(1, actual.getResolvedQualifiers().get(3).getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 4, asList("price_org"), -1)), asList(actual.getResolvedQualifiers().get(3).getResolvePaths()));
        // s.id
        assertEquals(-1, actual.getResolvedQualifiers().get(4).getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 0, asList("id"), -1)), asList(actual.getResolvedQualifiers().get(4).getResolvePaths()));

        e = e("aa.field.unknown");
        actual = SelectResolver.resolveTorTest(e, source);
        // aa.field.unknown
        assertEquals(asList(new ResolvePath(-1, 3, asList("field", "unknown"), -1)), asList(actual.getResolvedQualifiers().get(0).getResolvePaths()));

        e = e("aa.filter(aa -> aa.sku_id > 0)");
        actual = SelectResolver.resolveTorTest(e, source);
        // aa
        assertEquals(asList(new ResolvePath(-1, 2, emptyList(), -1)), asList(actual.getResolvedQualifiers().get(0).getResolvePaths()));
        // aa.sku_id
        assertEquals(asList(new ResolvePath(-1, 3, asList("sku_id"), -1)), asList(actual.getResolvedQualifiers().get(1).getResolvePaths()));

        e = e("filter(aa.filter(aa -> aa.sku_id > 0), aa -> aa.attr1_id > 0)");
        actual = SelectResolver.resolveTorTest(e, source);
        // aa
        assertEquals(asList(new ResolvePath(-1, 2, emptyList(), -1)), asList(actual.getResolvedQualifiers().get(0).getResolvePaths()));
        // aa.sku_id
        assertEquals(0, actual.getResolvedQualifiers().get(1).getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 3, asList("sku_id"), -1)), asList(actual.getResolvedQualifiers().get(1).getResolvePaths()));
        // aa.attr1_id
        assertEquals(0, actual.getResolvedQualifiers().get(2).getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 3, asList("attr1_id"), -1)), asList(actual.getResolvedQualifiers().get(2).getResolvePaths()));

        e = e("ap.sku_id = aa.sku_id");
        actual = SelectResolver.resolveTorTest(e, articlePrice);
        // ap.sku_id
        assertEquals(asList(new ResolvePath(-1, 4, asList("sku_id"), -1)), asList(actual.getResolvedQualifiers().get(0).getResolvePaths()));
        // aa.sku_id
        assertEquals(asList(new ResolvePath(-1, 3, asList("sku_id"), -1)), asList(actual.getResolvedQualifiers().get(1).getResolvePaths()));

        // Traverse up in hierarchy and then down
        e = e("aBrand.articleBrandId = a.articleBrandId");
        actual = SelectResolver.resolveTorTest(e, articleBrand);
        // aBrand.articleBrandId
        assertEquals(asList(new ResolvePath(-1, 6, asList("articleBrandId"), -1)), asList(actual.getResolvedQualifiers().get(0).getResolvePaths()));
        // a.articleBrandId
        assertEquals(asList(new ResolvePath(-1, 1, asList("articleBrandId"), -1)), asList(actual.getResolvedQualifiers().get(1).getResolvePaths()));

        e = e("aa.flatMap(x -> x.ap).map(x -> x.price_sales)");
        actual = SelectResolver.resolveTorTest(e, source);
        // aa
        assertEquals(asList(new ResolvePath(-1, 2, emptyList(), -1)), asList(actual.getResolvedQualifiers().get(0).getResolvePaths()));
        // x.ap
        assertEquals(0, actual.getResolvedQualifiers().get(1).getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 4, emptyList(), -1)), asList(actual.getResolvedQualifiers().get(1).getResolvePaths()));
        // x.price_sales
        assertEquals(0, actual.getResolvedQualifiers().get(2).getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 4, asList("price_sales"), -1)), asList(actual.getResolvedQualifiers().get(2).getResolvePaths()));
    }

    private Select s(String query)
    {
        return parser.parseSelect(session.getCatalogRegistry(), query);
    }

    private void assertResolveFail(Class<? extends Exception> e, String messageContains, Select select)
    {
        try
        {
            SelectResolver.resolve(session, select);
            fail("Resolve should fail with " + e);
        }
        catch (Exception ee)
        {
            if (!ee.getClass().isAssignableFrom(e))
            {
                throw ee;
            }

            assertTrue("Expected exception message to contain " + messageContains + " but was: " + ee.getMessage(), ee.getMessage().contains(messageContains));
        }
    }

    private void assertSelectFail(Class<? extends Exception> e, String messageContains, String query)
    {
        try
        {
            Select select = s(query);
            SelectResolver.resolve(session, select);
            fail("Resolve should fail with " + e);
        }
        catch (Exception ee)
        {
            if (!ee.getClass().isAssignableFrom(e))
            {
                throw ee;
            }

            assertTrue("Expected exception message to contain " + messageContains + " but was: " + ee.getMessage(), ee.getMessage().contains(messageContains));
        }
    }

    private Expression e(String expression)
    {
        return parser.parseExpression(session.getCatalogRegistry(), expression);
    }
}
