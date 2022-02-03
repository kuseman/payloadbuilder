package org.kuse.payloadbuilder.core.parser;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.kuse.payloadbuilder.core.catalog.TableMeta;
import org.kuse.payloadbuilder.core.catalog.TableMeta.DataType;
import org.kuse.payloadbuilder.core.parser.QualifiedReferenceExpression.ResolvePath;
import org.kuse.payloadbuilder.core.parser.rewrite.StatementResolver;

/** Test {@link StatementResolver} */
public class StatementResolverTest extends AParserTest
{
    @Test
    public void test_aggregating_select_items()
    {
        String query = "" +
            " select *, col2 "
            + "from ( "
            + "  select col1, col2"
            + "  from table b"
            + ") x ";

        QueryStatement stms = q(query);
        SelectStatement stm = (SelectStatement) stms.getStatements().get(0);

        List<SelectItem> selectItems = new ArrayList<>(stm.getSelect().getSelectItems());

        assertEquals(3, selectItems.size());

        List<QualifiedReferenceExpression> actual = getQualifiers(stm);

        // col1
        assertEquals(asList(new ResolvePath(-1, 1, asList("col1"), -1)), asList(actual.remove(0).getResolvePaths()));
        // col2
        assertEquals(asList(new ResolvePath(-1, 1, asList("col2"), -1)), asList(actual.remove(0).getResolvePaths()));
        // col2
        assertEquals(asList(new ResolvePath(-1, 1, asList("col2"), -1)), asList(actual.remove(0).getResolvePaths()));
    }

    @Test
    public void test_aggregating_select_items_2()
    {
        String query = "" +
            " select * "
            + "from tableA a "
            + "inner join tableB b "
            + "  on b.id = a.id ";

        QueryStatement stms = q(query);
        SelectStatement stm = (SelectStatement) stms.getStatements().get(0);

        List<SelectItem> selectItems = new ArrayList<>(stm.getSelect().getSelectItems());

        assertEquals(1, selectItems.size());

        // *
        AsteriskSelectItem ase = (AsteriskSelectItem) selectItems.remove(0);
        assertEquals(asList(0, 1), ase.getAliasTupleOrdinals());
    }

    @Test
    public void test_aggregating_select_items_3()
    {
        String query = "" +
            " select * "
            + "from tableA a "
            + "inner join "
            + "("
            + "  select * "
            + "  from tableA a"
            + "  inner join tableC c"
            + "    on c.id = a.id"
            + ") b "
            + "  on b.id = a.id ";

        QueryStatement stms = q(query);
        SelectStatement stm = (SelectStatement) stms.getStatements().get(0);

        List<SelectItem> selectItems = new ArrayList<>(stm.getSelect().getSelectItems());

        assertEquals(2, selectItems.size());

        // *
        AsteriskSelectItem ase = (AsteriskSelectItem) selectItems.remove(0);
        assertEquals(asList(0), ase.getAliasTupleOrdinals());
        // * (copied from sub query)
        ase = (AsteriskSelectItem) selectItems.remove(0);
        assertEquals(asList(2, 3), ase.getAliasTupleOrdinals());
    }

    @Test
    public void test_aggregating_select_items_4()
    {
        String query = "" +
            " select * "
            + "from "
            + "("
            + "  select * "
            + "  from tableA a"
            + "  inner join tableC c"
            + "    on c.id = a.id"
            + ") b "
            + "inner join tableA a "
            + "  on b.id = a.id ";

        QueryStatement stms = q(query);
        SelectStatement stm = (SelectStatement) stms.getStatements().get(0);

        List<SelectItem> selectItems = new ArrayList<>(stm.getSelect().getSelectItems());

        assertEquals(2, selectItems.size());

        // *
        AsteriskSelectItem ase = (AsteriskSelectItem) selectItems.remove(0);
        assertEquals(asList(3), ase.getAliasTupleOrdinals());
        // * (copied from sub query)
        ase = (AsteriskSelectItem) selectItems.remove(0);
        assertEquals(asList(1, 2), ase.getAliasTupleOrdinals());
    }

    @Test
    public void test_aggregating_select_items_5()
    {
        String query = "" +
            " select * "
            + "from "
            + "("
            + "  select * "
            + "  from tableA a"
            + "  inner join tableC c"
            + "    on c.id = a.id"
            + ") b "
            + "inner join "
            + "("
            + "  select * "
            + "  from tableA a"
            + "  inner join tableC c"
            + "    on c.id = a.id"
            + ") a "
            + "  on b.id = a.id ";

        QueryStatement stms = q(query);
        SelectStatement stm = (SelectStatement) stms.getStatements().get(0);

        List<SelectItem> selectItems = new ArrayList<>(stm.getSelect().getSelectItems());

        assertEquals(2, selectItems.size());

        // * (copied from sub query)
        AsteriskSelectItem ase = (AsteriskSelectItem) selectItems.remove(0);
        assertEquals(asList(1, 2), ase.getAliasTupleOrdinals());
        // * (copied from sub query)
        ase = (AsteriskSelectItem) selectItems.remove(0);
        assertEquals(asList(4, 5), ase.getAliasTupleOrdinals());
    }

    @Test
    public void test_aggregating_select_items_6()
    {
        String query = "" +
            " select * "
            + "from "
            + "("
            + "  select a.col, * "
            + "  from tableA a"
            + "  inner join tableC c"
            + "    on c.id = a.id"
            + ") b "
            + "inner join "
            + "("
            + "  select *, c.col2 "
            + "  from tableA a"
            + "  inner join tableC c"
            + "    on c.id = a.id"
            + ") a "
            + "  on b.id = a.id ";

        QueryStatement stms = q(query);
        SelectStatement stm = (SelectStatement) stms.getStatements().get(0);

        List<SelectItem> selectItems = new ArrayList<>(stm.getSelect().getSelectItems());

        assertEquals(4, selectItems.size());

        // a.col
        assertEquals(asList(new ResolvePath(-1, 1, asList("col"), -1)), asList(selectItems.remove(0).getResolvePaths()));
        // * (copied from sub query 1)
        AsteriskSelectItem ase = (AsteriskSelectItem) selectItems.remove(0);
        assertEquals(asList(1, 2), ase.getAliasTupleOrdinals());
        // * (copied from sub query 2)
        ase = (AsteriskSelectItem) selectItems.remove(0);
        assertEquals(asList(4, 5), ase.getAliasTupleOrdinals());
        // c.col2
        assertEquals(asList(new ResolvePath(-1, 5, asList("col2"), -1)), asList(selectItems.remove(0).getResolvePaths()));
    }

    @Test
    public void test_aggregating_select_items_7()
    {
        String query = " " +
            "select  * " +
            ",       col2   " +
            "from  " +
            "( " +
            "    select  col2     " +
            "    ,       col1 " +
            "    ,       * " +
            "    ,       col3     " +
            "    from  " +
            "    ( " +
            "        select  * " +
            "        ,       col4 " +
            "        ,       col10 + col11 calc " +
            "        ,       col5 " +
            "        ,       col6 " +
            "        from table " +
            "    ) y " +
            ") x";

        QueryStatement stms = q(query);
        SelectStatement stm = (SelectStatement) stms.getStatements().get(0);
        List<SelectItem> selectItems = new ArrayList<>(stm.getSelect().getSelectItems());

        assertEquals(9, selectItems.size());

        // col2
        assertEquals(asList(new ResolvePath(-1, 2, asList("col2"), -1)), asList(selectItems.remove(0).getResolvePaths()));
        // col1
        assertEquals(asList(new ResolvePath(-1, 2, asList("col1"), -1)), asList(selectItems.remove(0).getResolvePaths()));
        // *
        AsteriskSelectItem ase = (AsteriskSelectItem) selectItems.remove(0);
        assertEquals(asList(2), ase.getAliasTupleOrdinals());
        // col4
        assertEquals(asList(new ResolvePath(-1, 2, asList("col4"), -1)), asList(selectItems.remove(0).getResolvePaths()));
        // col10 + col11
        assertEquals(asList(new ResolvePath(-1, 2, asList(), TableMeta.MAX_COLUMNS)), asList(selectItems.remove(0).getResolvePaths()));
        // col5
        assertEquals(asList(new ResolvePath(-1, 2, asList("col5"), -1)), asList(selectItems.remove(0).getResolvePaths()));
        // col6
        assertEquals(asList(new ResolvePath(-1, 2, asList("col6"), -1)), asList(selectItems.remove(0).getResolvePaths()));
        // col3
        assertEquals(asList(new ResolvePath(-1, 2, asList("col3"), -1)), asList(selectItems.remove(0).getResolvePaths()));
        // col2
        assertEquals(asList(new ResolvePath(-1, 2, asList("col2"), -1)), asList(selectItems.remove(0).getResolvePaths()));
    }

    @Test
    public void test_column_resolving_with_case_insensitiveness()
    {
        QueryStatement stms = q("select x.vAlUe from range(1, 10) x");
        List<QualifiedReferenceExpression> actual = getQualifiers(stms.getStatements().get(0));
        // vAlUe
        assertEquals(asList(new ResolvePath(-1, 0, emptyList(), 0, DataType.INT)), asList(actual.get(0).getResolvePaths()));
    }

    @Test
    public void test_unknowm_column()
    {
        String query = "select x.dummy from range(1, 10) x";
        assertQueryFail(ParseException.class, "Unknown column: 'dummy' in table source: 'range (x)', expected one of: [Value]", query);
    }

    @Test
    public void test_expressions()
    {
        String query = ""
            + "select "
            + "art_id, "
            + "aa, "
            + "aa.ap, "
            + "aa.map(x -> x), "
            + "aa.map(x -> concat(x, ',')),"
            + "aa.map(x -> x.ap),"
            + "aa.map(x -> x.ap.price_sales),"
            + "unionall(aa, aa.ap),"
            + "unionall(aa, aa.ap).map(x -> x.s.art_id), "   // Traverse up to root and down
            + "unionall(aa, aa.ap).map(x -> x.art_id), "     // Combined columns from different aliases
            + "list.filter(l -> l.id > 0),"
            + "aa.map(aa -> aa.ap.map(ap -> ap.price_sales + ap.price_org)),"
            + "aa.flatmap(aa -> aa.ap.map(ap -> ap.price_sales + ap.price_org)),"
            + "aa.map(aa -> aa.ap.map(ap -> ap.price_sales + ap.price_org + s.id)),"
            + "aa.field.unknown,"
            + "filter(aa.filter(aa -> aa.sku_id > 0), aa -> aa.attr1_id > 0),"
            + "aa.flatMap(x -> x.ap).map(x -> x.price_sales),"
            + "map(case when s.active_flg then aa when a.active_flg then aa.ap else aa.ab end, x -> x.id) " // Combined columns from different aliases with case
            + "from source s "
            + "inner join article a "
            + "  on a.art_id = s.art_id "
            + "inner join ( "
            + "  select * "
            + "  from article_attribute aa "
            + "  inner join articleprice ap "
            + "    on ap.sku_id = aa.sku_id "
            + "  inner join articlebalance ab "
            + "    on ab.sku_id = ap.sku_id "
            + ") aa "
            + "  on aa.art_id = s.art_id "
            + "inner join articleBrand ab "
            + "  on ab.art_id = aa.art_id ";

        QueryStatement stms = q(query);
        List<QualifiedReferenceExpression> actual;
        QualifiedReferenceExpression re;

        actual = getQualifiers(stms.getStatements().get(0));
        assertEquals(57, actual.size());

        // art_id
        assertEquals(asList(new ResolvePath(-1, 0, asList("art_id"), -1)), asList(actual.remove(0).getResolvePaths()));
        // aa
        assertEquals(asList(new ResolvePath(-1, 2, asList(), -1, DataType.TUPLE)), asList(actual.remove(0).getResolvePaths()));
        // aa.ap
        assertEquals(asList(new ResolvePath(-1, 4, asList(), -1, DataType.TUPLE)), asList(actual.remove(0).getResolvePaths()));

        // aa.map(x -> x)
        // aa
        assertEquals(asList(new ResolvePath(-1, 2, asList(), -1, DataType.TUPLE)), asList(actual.remove(0).getResolvePaths()));
        // x
        re = actual.remove(0);
        assertEquals(0, re.getLambdaId());
        assertEquals(asList(new ResolvePath(-1, -1, asList(), -1)), asList(re.getResolvePaths()));

        // aa.map(x -> concat(x, ','))
        // aa
        assertEquals(asList(new ResolvePath(-1, 2, asList(), -1, DataType.TUPLE)), asList(actual.remove(0).getResolvePaths()));
        // x
        re = actual.remove(0);
        assertEquals(0, re.getLambdaId());
        assertEquals(asList(new ResolvePath(-1, -1, asList(), -1)), asList(re.getResolvePaths()));

        // aa.map(x -> x.ap)
        // aa
        assertEquals(asList(new ResolvePath(-1, 2, asList(), -1, DataType.TUPLE)), asList(actual.remove(0).getResolvePaths()));
        // x.ap
        re = actual.remove(0);
        assertEquals(0, re.getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 4, asList(), -1, DataType.TUPLE)), asList(re.getResolvePaths()));

        // aa.map(x -> x.ap.price_sales)
        // aa
        assertEquals(asList(new ResolvePath(-1, 2, asList(), -1, DataType.TUPLE)), asList(actual.remove(0).getResolvePaths()));
        // x.ap.price_sales
        re = actual.remove(0);
        assertEquals(0, re.getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 4, asList("price_sales"), -1)), asList(re.getResolvePaths()));

        // unionall(aa, aa.ap)
        // aa
        assertEquals(asList(new ResolvePath(-1, 2, asList(), -1, DataType.TUPLE)), asList(actual.remove(0).getResolvePaths()));
        // aa.ap
        assertEquals(asList(new ResolvePath(-1, 4, asList(), -1, DataType.TUPLE)), asList(actual.remove(0).getResolvePaths()));

        // unionall(aa, aa.ap).map(x -> x.s.art_id)
        // aa
        assertEquals(asList(new ResolvePath(-1, 2, asList(), -1, DataType.TUPLE)), asList(actual.remove(0).getResolvePaths()));
        // aa.ap
        assertEquals(asList(new ResolvePath(-1, 4, asList(), -1, DataType.TUPLE)), asList(actual.remove(0).getResolvePaths()));
        // x.s.art_id
        re = actual.remove(0);
        assertEquals(0, re.getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 0, asList("art_id"), -1)), asList(re.getResolvePaths()));

        // unionall(aa, aa.ap).map(x -> x.art_id)
        // aa
        assertEquals(asList(new ResolvePath(-1, 2, asList(), -1, DataType.TUPLE)), asList(actual.remove(0).getResolvePaths()));
        // aa.ap
        assertEquals(asList(new ResolvePath(-1, 4, asList(), -1, DataType.TUPLE)), asList(actual.remove(0).getResolvePaths()));
        // x.art_id
        re = actual.remove(0);
        assertEquals(0, re.getLambdaId());
        assertEquals(asList(
                new ResolvePath(2, 3, asList("art_id"), -1),
                new ResolvePath(4, 4, asList("art_id"), -1)), asList(re.getResolvePaths()));

        // list.filter(l -> l.id > 0)
        // list
        assertEquals(asList(new ResolvePath(-1, 0, asList("list"), -1)), asList(actual.remove(0).getResolvePaths()));
        // l.id
        re = actual.remove(0);
        assertEquals(0, re.getLambdaId());
        assertEquals(asList(new ResolvePath(-1, -1, asList("id"), -1)), asList(re.getResolvePaths()));

        // aa.map(aa -> aa.ap.map(ap -> ap.price_sales + ap.price_org))
        // aa
        assertEquals(asList(new ResolvePath(-1, 2, asList(), -1, DataType.TUPLE)), asList(actual.remove(0).getResolvePaths()));
        // aa.ap
        re = actual.remove(0);
        assertEquals(0, re.getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 4, asList(), -1, DataType.TUPLE)), asList(re.getResolvePaths()));
        // ap.price_sales
        re = actual.remove(0);
        assertEquals(1, re.getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 4, asList("price_sales"), -1)), asList(re.getResolvePaths()));
        // ap.price_org
        re = actual.remove(0);
        assertEquals(1, re.getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 4, asList("price_org"), -1)), asList(re.getResolvePaths()));

        // aa.flatmap(aa -> aa.ap.map(ap -> ap.price_sales + ap.price_org))
        // aa
        assertEquals(asList(new ResolvePath(-1, 2, asList(), -1, DataType.TUPLE)), asList(actual.remove(0).getResolvePaths()));
        // aa.ap
        re = actual.remove(0);
        assertEquals(0, re.getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 4, asList(), -1, DataType.TUPLE)), asList(re.getResolvePaths()));
        // ap.price_sales
        re = actual.remove(0);
        assertEquals(1, re.getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 4, asList("price_sales"), -1)), asList(re.getResolvePaths()));
        // ap.price_org
        re = actual.remove(0);
        assertEquals(1, re.getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 4, asList("price_org"), -1)), asList(re.getResolvePaths()));

        // aa.map(aa -> aa.ap.map(ap -> ap.price_sales + ap.price_org + s.id))
        // aa
        assertEquals(asList(new ResolvePath(-1, 2, asList(), -1, DataType.TUPLE)), asList(actual.remove(0).getResolvePaths()));
        // aa.ap
        re = actual.remove(0);
        assertEquals(0, re.getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 4, asList(), -1, DataType.TUPLE)), asList(re.getResolvePaths()));
        // ap.price_sales
        re = actual.remove(0);
        assertEquals(1, re.getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 4, asList("price_sales"), -1)), asList(re.getResolvePaths()));
        // ap.price_org
        re = actual.remove(0);
        assertEquals(1, re.getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 4, asList("price_org"), -1)), asList(re.getResolvePaths()));
        // s.id
        re = actual.remove(0);
        assertEquals(-1, re.getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 0, asList("id"), -1)), asList(re.getResolvePaths()));

        // aa.field.unknown
        assertEquals(asList(new ResolvePath(-1, 3, asList("field", "unknown"), -1)), asList(actual.remove(0).getResolvePaths()));

        // filter(aa.filter(aa -> aa.sku_id > 0), aa -> aa.attr1_id > 0)
        // aa
        assertEquals(asList(new ResolvePath(-1, 2, asList(), -1, DataType.TUPLE)), asList(actual.remove(0).getResolvePaths()));
        // aa.sku_id
        re = actual.remove(0);
        assertEquals(0, re.getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 3, asList("sku_id"), -1)), asList(re.getResolvePaths()));
        // aa.attr1_id
        re = actual.remove(0);
        assertEquals(0, re.getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 3, asList("attr1_id"), -1)), asList(re.getResolvePaths()));

        // aa.flatMap(x -> x.ap).map(x -> x.price_sales)
        // aa
        assertEquals(asList(new ResolvePath(-1, 2, asList(), -1, DataType.TUPLE)), asList(actual.remove(0).getResolvePaths()));
        // x.ap
        re = actual.remove(0);
        assertEquals(0, re.getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 4, asList(), -1, DataType.TUPLE)), asList(re.getResolvePaths()));
        // x.price_sales
        re = actual.remove(0);
        assertEquals(0, re.getLambdaId());
        assertEquals(asList(new ResolvePath(-1, 4, asList("price_sales"), -1)), asList(re.getResolvePaths()));

        // map(case when s.active_flg then aa when a.active_flg then aa.ap else aa.ab end, x -> x.id)
        // s.active_flg
        assertEquals(asList(new ResolvePath(-1, 0, asList("active_flg"), -1)), asList(actual.remove(0).getResolvePaths()));
        // aa
        assertEquals(asList(new ResolvePath(-1, 2, asList(), -1, DataType.TUPLE)), asList(actual.remove(0).getResolvePaths()));
        // a.active_flg
        assertEquals(asList(new ResolvePath(-1, 1, asList("active_flg"), -1)), asList(actual.remove(0).getResolvePaths()));
        // aa.ap
        assertEquals(asList(new ResolvePath(-1, 4, asList(), -1, DataType.TUPLE)), asList(actual.remove(0).getResolvePaths()));
        // aa.ab
        assertEquals(asList(new ResolvePath(-1, 5, asList(), -1, DataType.TUPLE)), asList(actual.remove(0).getResolvePaths()));
        // x.id
        re = actual.remove(0);
        assertEquals(0, re.getLambdaId());
        assertEquals(asList(
                new ResolvePath(2, 3, asList("id"), -1),
                new ResolvePath(4, 4, asList("id"), -1),
                new ResolvePath(5, 5, asList("id"), -1)), asList(re.getResolvePaths()));

        // a.art_id
        assertEquals(asList(new ResolvePath(-1, 1, asList("art_id"), -1)), asList(actual.remove(0).getResolvePaths()));
        // s.art_id
        assertEquals(asList(new ResolvePath(-1, 0, asList("art_id"), -1)), asList(actual.remove(0).getResolvePaths()));
        // ap.sku_id
        assertEquals(asList(new ResolvePath(-1, 4, asList("sku_id"), -1)), asList(actual.remove(0).getResolvePaths()));
        // aa.sku_id
        assertEquals(asList(new ResolvePath(-1, 3, asList("sku_id"), -1)), asList(actual.remove(0).getResolvePaths()));
        // ab.sku_id
        assertEquals(asList(new ResolvePath(-1, 5, asList("sku_id"), -1)), asList(actual.remove(0).getResolvePaths()));
        // ap.sku_id
        assertEquals(asList(new ResolvePath(-1, 4, asList("sku_id"), -1)), asList(actual.remove(0).getResolvePaths()));
        // aa.art_id
        assertEquals(asList(new ResolvePath(-1, 3, asList("art_id"), -1)), asList(actual.remove(0).getResolvePaths()));
        // s.art_id
        assertEquals(asList(new ResolvePath(-1, 0, asList("art_id"), -1)), asList(actual.remove(0).getResolvePaths()));
        // ab.art_id
        assertEquals(asList(new ResolvePath(-1, 6, asList("art_id"), -1)), asList(actual.remove(0).getResolvePaths()));
        // aa.art_id
        assertEquals(asList(new ResolvePath(-1, 3, asList("art_id"), -1)), asList(actual.remove(0).getResolvePaths()));
    }

    @Test
    public void test_temporary_tables()
    {
        String query = ""
            + "select aa.col5, bb.col6, bb, aa "
            + "into #temp1 "
            + "from tableAA aa "
            + "inner join tableBB bb "
            + "  on bb.col = aa.id "
            + ""
            + "select a.col, b.col2, b, x "
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
            + "    on t.col5 = d.id "
            + ") x"
            + "  on x.id = a.id "
            + " "
            + "select col, t.b, t.b.col3, t.x.d, t.x.t.bb, t.x.t.aa.col5, t.x.t.col5 "
            + "from #temp t ";

        QueryStatement stms = q(query);
        List<QualifiedReferenceExpression> actual;

        // 1:st statement
        actual = getQualifiers(stms.getStatements().get(0));
        assertEquals(6, actual.size());
        // aa.col5
        assertEquals(asList(new ResolvePath(-1, 0, asList("col5"), -1)), asList(actual.remove(0).getResolvePaths()));
        // bb.col6
        assertEquals(asList(new ResolvePath(-1, 1, asList("col6"), -1)), asList(actual.remove(0).getResolvePaths()));
        // bb
        assertEquals(asList(new ResolvePath(-1, 1, asList(), -1, DataType.TUPLE)), asList(actual.remove(0).getResolvePaths()));
        // aa
        assertEquals(asList(new ResolvePath(-1, 0, asList(), -1, DataType.TUPLE)), asList(actual.remove(0).getResolvePaths()));
        // bb.col
        assertEquals(asList(new ResolvePath(-1, 1, asList("col"), -1)), asList(actual.remove(0).getResolvePaths()));
        // aa.id
        assertEquals(asList(new ResolvePath(-1, 0, asList("id"), -1)), asList(actual.remove(0).getResolvePaths()));

        // [a.col, b.col2, b, x, b.id, a.id, d.id, c.id, t.col5, d.id, x.id, a.id]

        // 2:nd statement
        actual = getQualifiers(stms.getStatements().get(1));
        assertEquals(12, actual.size());
        // a.col
        assertEquals(asList(new ResolvePath(-1, 0, asList("col"), -1)), asList(actual.get(0).getResolvePaths()));
        // b.col2
        assertEquals(asList(new ResolvePath(-1, 1, asList("col2"), -1)), asList(actual.get(1).getResolvePaths()));
        // b
        assertEquals(asList(new ResolvePath(-1, 1, asList(), -1, DataType.TUPLE)), asList(actual.get(2).getResolvePaths()));
        // x
        assertEquals(asList(new ResolvePath(-1, 2, asList(), -1, DataType.TUPLE)), asList(actual.get(3).getResolvePaths()));

        // b.id
        assertEquals(asList(new ResolvePath(-1, 1, asList("id"), -1)), asList(actual.get(4).getResolvePaths()));
        // a.id
        assertEquals(asList(new ResolvePath(-1, 0, asList("id"), -1)), asList(actual.get(5).getResolvePaths()));
        // d.id
        assertEquals(asList(new ResolvePath(-1, 4, asList("id"), -1)), asList(actual.get(6).getResolvePaths()));
        // c.id
        assertEquals(asList(new ResolvePath(-1, 3, asList("id"), -1)), asList(actual.get(7).getResolvePaths()));
        // t.col5
        assertEquals(asList(new ResolvePath(-1, 5, asList(), 0)), asList(actual.get(8).getResolvePaths()));
        // d.id
        assertEquals(asList(new ResolvePath(-1, 4, asList("id"), -1)), asList(actual.get(9).getResolvePaths()));
        // x.id
        assertEquals(asList(new ResolvePath(-1, 3, asList("id"), -1)), asList(actual.get(10).getResolvePaths()));
        // a.id
        assertEquals(asList(new ResolvePath(-1, 0, asList("id"), -1)), asList(actual.get(11).getResolvePaths()));

        // 3:nd statement
        actual = getQualifiers(stms.getStatements().get(2));
        assertEquals(7, actual.size());
        // col
        assertEquals(asList(new ResolvePath(-1, 0, emptyList(), 0)), asList(actual.get(0).getResolvePaths()));
        // t.b
        assertEquals(asList(new ResolvePath(-1, 0, emptyList(), 2, DataType.TUPLE)), asList(actual.get(1).getResolvePaths()));
        // t.b.col3
        assertEquals(asList(new ResolvePath(-1, 0, asList("col3"), 2)), asList(actual.get(2).getResolvePaths()));
        // t.x.d => resolve into temp table sub query
        assertEquals(asList(new ResolvePath(-1, 0, emptyList(), 3, DataType.TUPLE,
                new ResolvePath[] {
                        new ResolvePath(-1, 4, emptyList(), -1)
                })), asList(actual.get(3).getResolvePaths()));
        // t.x.t.bb => resolve into temp table in sub query and then into another temp table's alias
        assertEquals(asList(new ResolvePath(-1, 0, emptyList(), 3, DataType.TUPLE,
                new ResolvePath[] {
                        new ResolvePath(-1, 5, emptyList(), -1),
                        new ResolvePath(-1, -1, emptyList(), 2)
                })), asList(actual.get(4).getResolvePaths()));
        // t.x.t.aa.col5 => resolve into temp table in sub query and then into another temp table's alias with column
        assertEquals(asList(new ResolvePath(-1, 0, asList("col5"), 3,
                new ResolvePath[] {
                        new ResolvePath(-1, 5, emptyList(), -1),
                        new ResolvePath(-1, -1, emptyList(), 3)
                })), asList(actual.get(5).getResolvePaths()));
        // t.x.t.col5 => resolve into temp table in sub query and then into one of it's columns
        assertEquals(asList(new ResolvePath(-1, 0, emptyList(), 3,
                new ResolvePath[] {
                        new ResolvePath(-1, 5, emptyList(), -1),
                        new ResolvePath(-1, -1, emptyList(), 0)
                })), asList(actual.get(6).getResolvePaths()));
    }

    @Test
    public void test_temporary_tables_1()
    {
        String query = ""
            + "select col, col2, t "
            + "into #temp "
            + "from table t "
            + " "
            + "select t.col3 from #temp ";

        QueryStatement stms = q(query);
        List<QualifiedReferenceExpression> actual;

        // 1:st statement
        actual = getQualifiers(stms.getStatements().get(0));
        assertEquals(3, actual.size());

        // col
        assertEquals(asList(new ResolvePath(-1, 0, asList("col"), -1)), asList(actual.remove(0).getResolvePaths()));
        // col2
        assertEquals(asList(new ResolvePath(-1, 0, asList("col2"), -1)), asList(actual.remove(0).getResolvePaths()));
        // t
        assertEquals(asList(new ResolvePath(-1, 0, asList(), -1, DataType.TUPLE)), asList(actual.remove(0).getResolvePaths()));

        // 2:st statement
        actual = getQualifiers(stms.getStatements().get(1));
        assertEquals(1, actual.size());

        // col3
        assertEquals(asList(new ResolvePath(-1, 0, asList("col3"), 2)), asList(actual.remove(0).getResolvePaths()));
    }

    @Test
    public void test_temporary_tables_no_column()
    {
        String query = ""
            + "select 1 col, 2 col2 "
            + "into #temp "
            + " "
            + "select col3 from #temp ";

        assertQueryFail(ParseException.class, "Unknown column: 'col3' in table source: '#temp', expected one of: [col, col2]", query);
    }

    @Test
    public void test_alias_policy()
    {
        s("select art_id from article");
        s("select art_id from article where a.id = 10");
        assertQueryFail(ParseException.class, "Invalid table source reference 'b'", "select art_id from article a where b.art_id > 10");
        assertQueryFail(ParseException.class, "Alias is mandatory", "select * from tableA inner join tableB b on b.art_id = art_id");
        assertQueryFail(ParseException.class, "Alias is mandatory", "select * from tableA a inner join tableB on b.art_id = art_id");
        assertQueryFail(ParseException.class, "Invalid table source reference 'q'", "select art_id from article a inner join (select * from tableB b where id= 1 and q.id = 10) b on b.id = a.id");
        // Test reference to a parent is ok
        s("select art_id from article a inner join (select * from tableB b where id= 1 and a.id = 10) b on b.id = a.id");
        // c is not allowed since it's defined later that current context alias
        assertQueryFail(ParseException.class, "Invalid table source reference 'c'",
                "select art_id from article a inner join (select * from tableB b where id= 1 and c.id = 10) b on b.id = a.id inner join tableC c on c.id = b.id");
    }

    @Test
    public void test_selects_fail()
    {
        assertQueryFail(ParseException.class, "Unkown reference 'col1'", "select col1,"
            + "(select col2, col3 where col5 != col6 for object) obj ");

        assertQueryFail(ParseException.class, "Unkown reference 'col1'", "select col1 + col2");

        // Make sure that expression sub query table aliases isn't accessible from outer scope
        assertQueryFail(ParseException.class, "Invalid table source reference 'b'", ""
            + "select col "
            + ", (select aCol1, aCol2, a.col3 from tableB b for object) obj "
            + "from tableA a "
            + "where b.col10 > 10");

        assertQueryFail(ParseException.class, "No temporary table found named '#tmp'", "select col1 from #tmp");
        assertQueryFail(ParseException.class, "Unkown reference 'col1'", "select col1");
        assertQueryFail(ParseException.class, "No alias found with name: a", "select a.* from table");
        assertQueryFail(ParseException.class, "Cannot use asterisk select without any table source", "select *");
    }

    @Test
    public void test_resolve_does_not_resolve_into_temp_table()
    {
        String query = ""
            + "select 1 obj, 2 col into #temp "
            + " "
            + "select ta.obj.nested.list.filter(x -> x.column = 1)[0].value "
            + "from #temp t "
            + "inner join table ta"
            + "  on ta.col = t.col";

        QueryStatement stms = q(query);
        QualifiedReferenceExpression re;

        List<QualifiedReferenceExpression> actual = getQualifiers(stms.getStatements().get(1));

        assertEquals(5, actual.size());

        // "ta.obj.nested.list.filter(x -> x.column = 1)[0].value"
        // ta.obj.nested.list
        assertEquals(asList(new ResolvePath(-1, 1, asList("obj", "nested", "list"), -1)), asList(actual.remove(0).getResolvePaths()));
        // x.column
        re = actual.remove(0);
        assertEquals(0, re.getLambdaId());
        assertEquals(asList(new ResolvePath(-1, -1, asList("column"), -1)), asList(re.getResolvePaths()));
        // value
        // value is unknown and hence should have target tuple -1, ie we
        // should try to resolve value from whatever is on the left hand side
        assertEquals(asList(new ResolvePath(-1, -1, asList("value"), -1)), asList(actual.remove(0).getResolvePaths()));
        // ta.col
        assertEquals(asList(new ResolvePath(-1, 1, asList("col"), -1)), asList(actual.remove(0).getResolvePaths()));
        // t.col
        assertEquals(asList(new ResolvePath(-1, 0, asList(), 1)), asList(actual.remove(0).getResolvePaths()));
    }

    @Test
    public void test_selects()
    {
        String query;
        List<QualifiedReferenceExpression> actual;
        QueryStatement stms;

        query = ""
            + "select col1,"
            + "(select col2, col3 where col5 != col6 for object) obj "
            + "from table";

        stms = q(query);
        actual = getQualifiers(stms.getStatements().get(0));
        assertEquals(5, actual.size());
        // col1
        assertEquals(asList(new ResolvePath(-1, 0, asList("col1"), -1)), asList(actual.remove(0).getResolvePaths()));
        // col2
        assertEquals(asList(new ResolvePath(-1, 0, asList("col2"), -1)), asList(actual.remove(0).getResolvePaths()));
        // col3
        assertEquals(asList(new ResolvePath(-1, 0, asList("col3"), -1)), asList(actual.remove(0).getResolvePaths()));
        // col5
        assertEquals(asList(new ResolvePath(-1, 0, asList("col5"), -1)), asList(actual.remove(0).getResolvePaths()));
        // col6
        assertEquals(asList(new ResolvePath(-1, 0, asList("col6"), -1)), asList(actual.remove(0).getResolvePaths()));

        query = "select col1,"
            + "(select col2, col3 where col5 != col6 and b.col7 != 'value' for object) obj "
            + "from tableA a "
            + "inner join tableB b "
            + "  on b.id = a.id";

        stms = q(query);
        actual = getQualifiers(stms.getStatements().get(0));
        assertEquals(8, actual.size());
        // col1
        assertEquals(asList(new ResolvePath(-1, 0, asList("col1"), -1)), asList(actual.remove(0).getResolvePaths()));
        // col2
        assertEquals(asList(new ResolvePath(-1, 0, asList("col2"), -1)), asList(actual.remove(0).getResolvePaths()));
        // col3
        assertEquals(asList(new ResolvePath(-1, 0, asList("col3"), -1)), asList(actual.remove(0).getResolvePaths()));
        // col5
        assertEquals(asList(new ResolvePath(-1, 0, asList("col5"), -1)), asList(actual.remove(0).getResolvePaths()));
        // col6
        assertEquals(asList(new ResolvePath(-1, 0, asList("col6"), -1)), asList(actual.remove(0).getResolvePaths()));
        // col7
        assertEquals(asList(new ResolvePath(-1, 1, asList("col7"), -1)), asList(actual.remove(0).getResolvePaths()));
        // b.id
        assertEquals(asList(new ResolvePath(-1, 1, asList("id"), -1)), asList(actual.remove(0).getResolvePaths()));
        // a.id
        assertEquals(asList(new ResolvePath(-1, 0, asList("id"), -1)), asList(actual.remove(0).getResolvePaths()));

        query = "select col1, "
            + "(select col2, col3 where col5 != col6 and b.col7 != 'value' for object) object, "
            // Acess to subquery column AND outer scope column from tableB
            + "(select Value, b.col10, b.id from range(1,10) for array) array "
            + "from tableA a "
            + "inner join tableB b "
            + "  on b.id = a.id";

        stms = q(query);
        actual = getQualifiers(stms.getStatements().get(0));
        assertEquals(11, actual.size());

        // col1
        assertEquals(asList(new ResolvePath(-1, 0, asList("col1"), -1)), asList(actual.remove(0).getResolvePaths()));
        // col2
        assertEquals(asList(new ResolvePath(-1, 0, asList("col2"), -1)), asList(actual.remove(0).getResolvePaths()));
        // col3
        assertEquals(asList(new ResolvePath(-1, 0, asList("col3"), -1)), asList(actual.remove(0).getResolvePaths()));
        // col5
        assertEquals(asList(new ResolvePath(-1, 0, asList("col5"), -1)), asList(actual.remove(0).getResolvePaths()));
        // col6
        assertEquals(asList(new ResolvePath(-1, 0, asList("col6"), -1)), asList(actual.remove(0).getResolvePaths()));
        // col7
        assertEquals(asList(new ResolvePath(-1, 1, asList("col7"), -1)), asList(actual.remove(0).getResolvePaths()));
        // Value
        assertEquals(asList(new ResolvePath(-1, 4, asList(), 0, DataType.INT)), asList(actual.remove(0).getResolvePaths()));
        // b.col10
        assertEquals(asList(new ResolvePath(-1, 1, asList("col10"), -1)), asList(actual.remove(0).getResolvePaths()));
        // b.id
        assertEquals(asList(new ResolvePath(-1, 1, asList("id"), -1)), asList(actual.remove(0).getResolvePaths()));
        // b.id
        assertEquals(asList(new ResolvePath(-1, 1, asList("id"), -1)), asList(actual.remove(0).getResolvePaths()));
        // a.id
        assertEquals(asList(new ResolvePath(-1, 0, asList("id"), -1)), asList(actual.remove(0).getResolvePaths()));

        query = "select col, x.col2.key "
            + "from "
            + "( "
            + "  select 'value' col, "
            + "        (select 123 key for object) col2"
            + ") x";

        stms = q(query);

        actual = getQualifiers(stms.getStatements().get(0));
        // Only one reference left since col is aggregated to root
        assertEquals(2, actual.size());
        // col
        assertEquals(asList(new ResolvePath(-1, 0, asList(), 0)), asList(actual.remove(0).getResolvePaths()));
        // x.col2.key
        assertEquals(asList(new ResolvePath(-1, 0, asList("key"), 1)), asList(actual.remove(0).getResolvePaths()));

        query = "select col, x.map.key "
            + "from "
            + "( "
            + "  select col, map "
            + "  from table"
            + ") x";

        stms = q(query);

        actual = getQualifiers(stms.getStatements().get(0));
        // Only one reference left since col is aggregated to root
        assertEquals(4, actual.size());
        // col
        assertEquals(asList(new ResolvePath(-1, 1, asList("col"), -1)), asList(actual.remove(0).getResolvePaths()));
        // x.map.key
        assertEquals(asList(new ResolvePath(-1, 1, asList("map", "key"), -1)), asList(actual.remove(0).getResolvePaths()));
        // x.map.key
        assertEquals(asList(new ResolvePath(-1, 1, asList("col"), -1)), asList(actual.remove(0).getResolvePaths()));
        // map
        assertEquals(asList(new ResolvePath(-1, 1, asList("map"), -1)), asList(actual.remove(0).getResolvePaths()));

        // Verify select items
        List<SelectItem> selectItems = ((SelectStatement) stms.getStatements().get(0)).getSelect().getSelectItems();
        assertEquals(2, selectItems.size());
        // col
        assertEquals(asList(new ResolvePath(-1, 1, asList("col"), -1)), asList(selectItems.remove(0).getResolvePaths()));
        // x.map.key
        assertEquals(asList(new ResolvePath(-1, 1, asList("map", "key"), -1)), asList(selectItems.remove(0).getResolvePaths()));

        query = "select * " +
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
            "  on  x.map.key = v.Value";

        stms = q(query);
        actual = getQualifiers(stms.getStatements().get(0));
        assertEquals(5, actual.size());

        //        [expr_2_0, expr_2_0, Value, x.map.key, v.Value]

        // expr_2_0 (copied root select item
        assertEquals(asList(new ResolvePath(-1, 2, asList(), 1_000_000)), asList(actual.remove(0).getResolvePaths()));
        // expr_2_0 (extracted computed expression in sub query)
        assertEquals(asList(new ResolvePath(-1, 2, asList(), 1_000_000)), asList(actual.remove(0).getResolvePaths()));
        // Value (inner most sub query)
        assertEquals(asList(new ResolvePath(-1, 2, emptyList(), 0, DataType.INT)), asList(actual.remove(0).getResolvePaths()));
        // x.map.key
        assertEquals(asList(new ResolvePath(-1, 2, asList("key"), 1_000_000)), asList(actual.remove(0).getResolvePaths()));
        // v.Value
        assertEquals(asList(new ResolvePath(-1, 0, emptyList(), 0, DataType.INT)), asList(actual.remove(0).getResolvePaths()));

        // Nested TVF's
        query = "select Value, " +
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
            "  on  x.Value = v.Value";

        stms = q(query);
        actual = getQualifiers(stms.getStatements().get(0));
        assertEquals(9, actual.size());

        // Value (top select)
        assertEquals(asList(new ResolvePath(-1, 0, emptyList(), 0, DataType.INT)), asList(actual.remove(0).getResolvePaths()));
        // Value (first sub query select => x.Value)
        assertEquals(asList(new ResolvePath(-1, 2, emptyList(), 0, DataType.INT)), asList(actual.remove(0).getResolvePaths()));
        // Value (second sub query select -> r1.Value)
        assertEquals(asList(new ResolvePath(-1, 3, emptyList(), 0, DataType.INT)), asList(actual.remove(0).getResolvePaths()));
        // r1 (open_rows(r1)
        assertEquals(asList(new ResolvePath(-1, 3, emptyList(), -1, DataType.TUPLE)), asList(actual.remove(0).getResolvePaths()));
        // x (open_rows(x))
        assertEquals(asList(new ResolvePath(-1, 1, emptyList(), -1, DataType.TUPLE)), asList(actual.remove(0).getResolvePaths()));
        // r1.Value
        assertEquals(asList(new ResolvePath(-1, 3, emptyList(), 0, DataType.INT)), asList(actual.remove(0).getResolvePaths()));
        // r.Value
        assertEquals(asList(new ResolvePath(-1, 2, emptyList(), 0, DataType.INT)), asList(actual.remove(0).getResolvePaths()));
        // x.Value
        assertEquals(asList(new ResolvePath(-1, 2, emptyList(), 0, DataType.INT)), asList(actual.remove(0).getResolvePaths()));
        // v.Value
        assertEquals(asList(new ResolvePath(-1, 0, emptyList(), 0, DataType.INT)), asList(actual.remove(0).getResolvePaths()));
    }

    private List<QualifiedReferenceExpression> getQualifiers(Statement statement)
    {
        List<QualifiedReferenceExpression> result = new ArrayList<>();

        final AtomicReference<AExpressionVisitor<Void, Void>> expressionVisitor = new AtomicReference<>();
        final AStatementVisitor<Void, Void> statementVisitor = new AStatementVisitor<Void, Void>()
        {
            @Override
            protected void visitExpression(Void context, Expression expression)
            {
                expression.accept(expressionVisitor.get(), null);
            }
        };

        expressionVisitor.set(new AExpressionVisitor<Void, Void>()
        {
            @Override
            public Void visit(QualifiedReferenceExpression expression, Void context)
            {
                result.add(expression);
                return null;
            }

            @Override
            public Void visit(UnresolvedSubQueryExpression expression, Void context)
            {
                expression.getSelectStatement().accept(statementVisitor, null);
                return null;
            }
        });

        statement.accept(statementVisitor, null);
        return result;
    }
}
