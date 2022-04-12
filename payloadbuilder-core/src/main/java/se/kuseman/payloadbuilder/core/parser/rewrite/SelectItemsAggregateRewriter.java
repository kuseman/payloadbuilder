package se.kuseman.payloadbuilder.core.parser.rewrite;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import se.kuseman.payloadbuilder.core.parser.AJoin;
import se.kuseman.payloadbuilder.core.parser.AsteriskSelectItem;
import se.kuseman.payloadbuilder.core.parser.Select;
import se.kuseman.payloadbuilder.core.parser.SelectItem;
import se.kuseman.payloadbuilder.core.parser.SubQueryTableSource;
import se.kuseman.payloadbuilder.core.parser.TableSource;
import se.kuseman.payloadbuilder.core.parser.TableSourceJoined;
import se.kuseman.payloadbuilder.core.parser.rewrite.StatementResolver.Context;
import se.kuseman.payloadbuilder.core.parser.rewrite.StatementResolver.SelectRewriter;

/**
 * Rewrite rule that aggregates select items from child sub query if any.
 */
class SelectItemsAggregateRewriter implements SelectRewriter<Select>
{
    // CSOFF
    @Override
    // CSON
    public Select rewrite(Select source, Context context)
    {
        if (!shouldAggregate(source))
        {
            return source;
        }

        /*
         * Aggregate select items from sub queries from either the table source or joins
         *
         * select * (ordinals = 0, 1) from table a inner join ( select * (ordinals = 2, 3) from tableB b inner join tableC on ... ) x
         *
         * Top asterisk should be replaced with ordinals = 0,2,3
         *
         * ==============================
         *
         * select *, x.id (ordinals = 0, 3) from ( select * (ordinals = 1, 2) from tableA a inner join tableB b on ..... ) a inner join ( select * (ordinals = 4, 5) from tableB b inner join tableC on
         * ... ) x
         *
         * Top asterisk should be replaced with ordinals = 1,2,4,5
         *
         * ============================== select * from ( select col1,col2,b.* from table a inner join tableb b on ..... ) x
         *
         */

        List<SelectItem> selectItems = new ArrayList<>();

        for (SelectItem item : source.getSelectItems())
        {
            if (item.isAsterisk())
            {
                AsteriskSelectItem asi = (AsteriskSelectItem) item;
                Set<Integer> targetTupleOrdinals = new HashSet<>(asi.getAliasTupleOrdinals());
                List<SelectItem> resultItems = new ArrayList<>();
                List<Integer> resultOrdinals = new ArrayList<>();
                aggregateSelectItems(targetTupleOrdinals, resultItems, resultOrdinals, source.getFrom());

                // If the resulting ordinals contains any of the current asterisk
                // ordinals, then add it to resulting select items
                for (Integer ordinal : resultOrdinals)
                {
                    if (targetTupleOrdinals.contains(ordinal))
                    {
                        selectItems.add(new AsteriskSelectItem(asi.getAlias(), asi.getToken(), resultOrdinals));
                        break;
                    }
                }

                // Add any resulting items found
                selectItems.addAll(resultItems);
            }
            else
            {
                selectItems.add(item);
            }
        }

        return new Select(selectItems, source.getFrom(), source.getInto(), source.getTopExpression(), source.getWhere(), source.getGroupBy(), source.getOrderBy(), source.getForOutput(),
                source.getComputedExpressions());
    }

    private boolean shouldAggregate(Select select)
    {
        if (select.getFrom() == null)
        {
            return false;
        }

        if (select.getFrom()
                .getTableSource() instanceof SubQueryTableSource)
        {
            return true;
        }

        for (AJoin join : select.getFrom()
                .getJoins())
        {
            if (join.getTableSource() instanceof SubQueryTableSource)
            {
                return true;
            }
        }

        return false;
    }

    private void aggregateSelectItems(Set<Integer> asteriskAliasOrdinals, List<SelectItem> selectItems, List<Integer> ordinals, TableSource ts)
    {
        if (!asteriskAliasOrdinals.contains(ts.getTableAlias()
                .getTupleOrdinal()))
        {
            return;
        }

        /*
         * Sub query, then pick all select items from sub query If an asterisk item is encountered, add it's ordinals to result list
         */
        if (ts instanceof SubQueryTableSource)
        {
            SubQueryTableSource sts = (SubQueryTableSource) ts;
            // Add all sub query items
            selectItems.addAll(sts.getSelect()
                    .getSelectItems());
        }
        /* Ordinary table source, just add it's ordinal to asterisk list */
        else
        {
            ordinals.add(ts.getTableAlias()
                    .getTupleOrdinal());
        }
    }

    private void aggregateSelectItems(Set<Integer> asteriskAliasOrdinals, List<SelectItem> selectItems, List<Integer> ordinals, TableSourceJoined tableSourceJoined)
    {
        aggregateSelectItems(asteriskAliasOrdinals, selectItems, ordinals, tableSourceJoined.getTableSource());
        for (AJoin join : tableSourceJoined.getJoins())
        {
            aggregateSelectItems(asteriskAliasOrdinals, selectItems, ordinals, join.getTableSource());
        }
    }
}
