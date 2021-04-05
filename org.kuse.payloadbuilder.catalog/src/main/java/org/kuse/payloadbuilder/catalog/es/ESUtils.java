package org.kuse.payloadbuilder.catalog.es;

import static java.util.Arrays.asList;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;

/** Utils for ESCatalog */
final class ESUtils
{
    private ESUtils()
    {
    }

    /** Build search body **/
    static String getSearchBody(
            List<Pair<String, String>> sortItems,
            List<PropertyPredicate> propertyPredicates,
            boolean isSingleType,
            ExecutionContext context)
    {
        StringBuilder sb = new StringBuilder("{");
        appendSortItems(sortItems, sb);
        appendPropertyPredicates(propertyPredicates, isSingleType, sb, context);
        sb.append("}");
        return sb.toString();
    }

    private static void appendPropertyPredicates(
            List<PropertyPredicate> propertyPredicates,
            boolean isSIngleType,
            StringBuilder sb,
            ExecutionContext context)
    {
        if (propertyPredicates.isEmpty())
        {
            return;
        }

        sb.append(",");

        StringBuilder filterMust = new StringBuilder();
        StringBuilder filterMustNot = new StringBuilder();

        List<StringBuilder> sbs = asList(filterMust, filterMustNot);
        int length = sbs.size();
        for (PropertyPredicate predicate : propertyPredicates)
        {
            predicate.appendBooleanClause(
                    filterMust,
                    filterMustNot,
                    context);

            for (int i = 0; i < length; i++)
            {
                StringBuilder sbp = sbs.get(i);
                if (sbp.length() > 1 && sbp.charAt(sbp.length() - 1) != ',')
                {
                    sbp.append(",");
                }
            }
        }

        for (int i = 0; i < length; i++)
        {
            stripLastComma(sbs.get(i));
        }

        if (isSIngleType)
        {
            appendSingleTypePredicates(sb, filterMust, filterMustNot);
        }
        else
        {
            appendNonSingleTypePredicate(sb, filterMust, filterMustNot);
        }
    }

    private static void appendSortItems(List<Pair<String, String>> sortItems, StringBuilder sb)
    {
        sb.append("\"sort\":[");
        if (sortItems.isEmpty())
        {
            sb.append("\"_doc\"]");
            return;
        }

        for (Pair<String, String> sortItem : sortItems)
        {
            // { "field": { "order": "desc" } }
            sb.append("{\"")
                    .append(sortItem.getKey())
                    .append("\":{\"order\":\"")
                    .append(sortItem.getValue())
                    .append("\"}}")
                    .append(",");
        }
        stripLastComma(sb);
        sb.append("]");
    }

    //CSOFF
    private static void appendNonSingleTypePredicate(
            //CSON
            StringBuilder sb,
            StringBuilder filterMust,
            StringBuilder filterMustNot)
    {
        boolean addComma = false;
        sb.append("\"filter\":{\"bool\":{");
        if (filterMust.length() > 0)
        {
            addComma = true;
            sb.append("\"must\":[");
            sb.append(filterMust);
            sb.append("]");
        }
        if (filterMustNot.length() > 0)
        {
            //CSOFF
            if (addComma)
            //CSON
            {
                sb.append(",");
            }
            sb.append("\"must_not\":[");
            sb.append(filterMustNot);
            sb.append("]");
        }

        sb.append("}}");
    }

    /** Build query for ES version >= 7 */
    private static void appendSingleTypePredicates(
            StringBuilder sb,
            StringBuilder filterMust,
            StringBuilder filterMustNot)
    {
        sb.append("\"query\":{\"bool\":{");

        if (filterMust.length() > 0)
        {
            sb.append("\"filter\":[");
            sb.append(filterMust);
            sb.append("],");
        }

        if (filterMustNot.length() > 0)
        {
            sb.append("\"must_not\":[");

            if (filterMustNot.length() > 0)
            {
                sb.append(filterMustNot);
            }
            sb.append("],");
        }
        stripLastComma(sb);
        sb.append("}}");
    }

    private static void stripLastComma(StringBuilder sb)
    {
        if (sb.length() >= 1 && sb.charAt(sb.length() - 1) == ',')
        {
            sb.deleteCharAt(sb.length() - 1);
        }
    }
}
