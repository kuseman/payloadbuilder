package org.kuse.payloadbuilder.core.parser.rewrite;

import static java.util.Collections.emptyList;
import static org.apache.commons.lang3.StringUtils.lowerCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.kuse.payloadbuilder.core.operator.TableMeta;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.ExpressionSelectItem;
import org.kuse.payloadbuilder.core.parser.LiteralIntegerExpression;
import org.kuse.payloadbuilder.core.parser.ParseException;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.parser.QualifiedReferenceExpression;
import org.kuse.payloadbuilder.core.parser.QualifiedReferenceExpression.ResolvePath;
import org.kuse.payloadbuilder.core.parser.Select;
import org.kuse.payloadbuilder.core.parser.SelectItem;
import org.kuse.payloadbuilder.core.parser.SortItem;
import org.kuse.payloadbuilder.core.parser.rewrite.StatementResolver.Context;
import org.kuse.payloadbuilder.core.parser.rewrite.StatementResolver.SelectRewriter;

/**
 * Rewriter for selects with order by items. Switches order by item when order by ordinal is used
 */
class OrderBySelectItemRewriter implements SelectRewriter<Select>
{
    @Override
    public Select rewrite(Select source, Context context)
    {
        if (source.getOrderBy().isEmpty())
        {
            return source;
        }

        int size = source.getSelectItems().size();
        Map<String, Integer> selectItemByIdentifier = new HashMap<>(size);

        int firstAsteriskOrdinal = -1;
        for (int i = 0; i < size; i++)
        {
            SelectItem item = source.getSelectItems().get(i);

            if (firstAsteriskOrdinal == -1 && item.isAsterisk())
            {
                firstAsteriskOrdinal = i + 1;
            }

            if (!item.isEmptyIdentifier())
            {
                selectItemByIdentifier.put(lowerCase(item.getIdentifier()), i);
            }
        }

        List<SelectItem> newSelectItems = new ArrayList<>(source.getSelectItems());
        List<SortItem> newSortItems = new ArrayList<>(source.getOrderBy());
        List<Expression> newComputedExpressions = new ArrayList<>(source.getComputedExpressions());

        size = newSortItems.size();
        for (int i = 0; i < size; i++)
        {
            SortItem si = newSortItems.get(i);

            // Order by ordinal
            if (si.getExpression() instanceof LiteralIntegerExpression)
            {
                // Computed expression in select item, then back one step
                // in loop and let it be handled in other if-statement
                if (processOrderByOrdinal(source, i, si, firstAsteriskOrdinal, newSortItems))
                {
                    i--;
                }
            }
            // Order by column
            else if (si.getExpression() instanceof QualifiedReferenceExpression)
            {
                processOrderByColumn(
                        source,
                        i,
                        si,
                        selectItemByIdentifier,
                        newSelectItems,
                        newSortItems,
                        newComputedExpressions);
            }
        }

        return new Select(
                newSelectItems,
                source.getFrom(),
                source.getInto(),
                source.getTopExpression(),
                source.getWhere(),
                source.getGroupBy(),
                newSortItems,
                source.getForOutput(),
                newComputedExpressions);
    }

    private boolean processOrderByOrdinal(
            Select source,
            int index,
            SortItem si,
            int firstAsteriskOrdinal,
            List<SortItem> sortItems)
    {
        int ordinal = ((LiteralIntegerExpression) si.getExpression()).getValue();

        // Ordinal is 1-based
        if (ordinal < 1 || ordinal > source.getSelectItems().size())
        {
            throw new ParseException("ORDER BY position is out of range", si.getToken());
        }

        // There cannot be a asterisk on the ordinal or prior to the ordinal
        if (firstAsteriskOrdinal != -1 && ordinal >= firstAsteriskOrdinal)
        {
            throw new ParseException("ORDER BY position is not supported for non expression select items", si.getToken());
        }

        ExpressionSelectItem selectItem = (ExpressionSelectItem) source.getSelectItems().get(ordinal - 1);

        // Replace the ordinal sort item with a expression sort item
        sortItems.set(index, new SortItem(
                selectItem.getExpression(),
                si.getOrder(),
                si.getNullOrder(),
                si.getToken()));

        return selectItem.isComputed();
    }

    private void processOrderByColumn(
            Select source,
            int index,
            SortItem si,
            Map<String, Integer> computedItemByIdentifier,
            List<SelectItem> newSelectItems,
            List<SortItem> newSortItems,
            List<Expression> newComputedExpressions)
    {
        QualifiedReferenceExpression qre = (QualifiedReferenceExpression) si.getExpression();
        QualifiedName qname = qre.getQname();

        if (qname.getParts().size() != 1)
        {
            return;
        }

        Integer itemIndex = computedItemByIdentifier.get(lowerCase(qname.getFirst()));

        // Not a pointer to a computed expression
        if (itemIndex == null)
        {
            return;
        }

        ExpressionSelectItem esi = (ExpressionSelectItem) newSelectItems.get(itemIndex);

        // Computed expression
        // then add expression to computed list
        // and switch sort item and select item to the computed
        if (esi.isComputed())
        {
            int tupleOrdinal = source.getFrom() != null
                ? source.getFrom().getTableSource().getTableAlias().getTupleOrdinal()
                : -1;
            QualifiedReferenceExpression newQre = new QualifiedReferenceExpression(
                    QualifiedName.of(String.format("expr_%d_%d", tupleOrdinal, newComputedExpressions.size())),
                    -1,
                    new ResolvePath[] {
                            new ResolvePath(-1, tupleOrdinal, emptyList(), TableMeta.MAX_COLUMNS + newComputedExpressions.size())
                    },
                    null);

            newComputedExpressions.add(esi.getExpression());

            newSelectItems.set(itemIndex, new ExpressionSelectItem(
                    newQre,
                    esi.isEmptyIdentifier(),
                    esi.getIdentifier(),
                    null,
                    esi.getToken()));

            newSortItems.set(index, new SortItem(
                    newQre,
                    si.getOrder(),
                    si.getNullOrder(),
                    si.getToken()));
        }
        // Non computed, simple change the sort item to the select items expression
        else
        {
            newSortItems.set(index, new SortItem(
                    esi.getExpression(),
                    si.getOrder(),
                    si.getNullOrder(),
                    si.getToken()));
        }
    }
}
