package org.kuse.payloadbuilder.catalog.es;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.List;

import org.apache.http.client.utils.URIBuilder;
import org.kuse.payloadbuilder.catalog.es.ESCatalog.MappedProperty;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.operator.IOrdinalValuesFactory.IOrdinalValues;
import org.kuse.payloadbuilder.core.parser.SortItem.NullOrder;
import org.kuse.payloadbuilder.core.parser.SortItem.Order;

/** Utils for ESCatalog */
final class ESUtils
{
    private ESUtils()
    {
    }

    static URIBuilder uriBuilder(String uri)
    {
        try
        {
            return new URIBuilder(uri);
        }
        catch (URISyntaxException e)
        {
            throw new RuntimeException("Error creating URIBuilder", e);
        }
    }

    static String toUrl(URIBuilder builder)
    {
        try
        {
            return builder.build().toURL().toExternalForm();
        }
        catch (MalformedURLException | URISyntaxException e)
        {
            throw new RuntimeException("Error creating URL", e);
        }
    }

    /** Build search body **/
    static String getSearchBody(
            List<SortItemMeta> sortItems,
            List<PropertyPredicate> propertyPredicates,
            boolean isSingleType,
            ExecutionContext context)
    {
        StringBuilder sb = new StringBuilder("{");
        appendSortItems(sortItems, sb, isSingleType);
        appendPropertyPredicates(propertyPredicates, isSingleType, sb, context);
        sb.append("}");
        return sb.toString();
    }

    /** Build search body for index query with terms. Used when mgets cannot be used */
    static String getIndexSearchBody(
            ExecutionContext context,
            boolean quoteValues,
            String field,
            boolean isSingleType)
    {
        StringBuilder sb = new StringBuilder("{");

        if (isSingleType)
        {
            sb.append("\"query\":{\"bool\":{\"must\":[{\"terms\":{\"");
        }
        else
        {
            sb.append("\"filter\":{\"bool\":{\"must\":[{\"terms\":{\"");
        }

        sb.append(field)
                .append("\":[");

        Iterator<IOrdinalValues> it = context.getStatementContext().getOuterOrdinalValues();
        while (it.hasNext())
        {
            Object val = it.next().getValue(0);
            if (val == null)
            {
                continue;
            }

            if (quoteValues)
            {
                sb.append("\"");
            }

            sb.append(val);

            if (quoteValues)
            {
                sb.append("\"");
            }
            if (it.hasNext())
            {
                sb.append(",");
            }
        }

        sb.append("]}}]}}}");
        return sb.toString();
    }

    private static void appendPropertyPredicates(
            List<PropertyPredicate> propertyPredicates,
            boolean isSingleType,
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

        if (isSingleType)
        {
            appendSingleTypePredicates(sb, filterMust, filterMustNot);
        }
        else
        {
            appendNonSingleTypePredicate(sb, filterMust, filterMustNot);
        }
    }

    private static void appendSortItems(List<SortItemMeta> sortItems, StringBuilder sb, boolean isSingleType)
    {
        sb.append("\"sort\":[");
        if (sortItems.isEmpty())
        {
            sb.append("\"_doc\"]");
            return;
        }

        for (SortItemMeta sortItem : sortItems)
        {
            MappedProperty property = sortItem.property;

            String field = sortItem.property.name;
            // Switch to a non free text field if exist
            if (property.isFreeTextMapping())
            {
                MappedProperty nonFreeTextField = sortItem.property.getNonFreeTextField();
                if (nonFreeTextField != null)
                {
                    field = nonFreeTextField.name;
                }
            }

            // { "field": { "order": "desc", "missing": "_last", "nested": { "path": "path" } } }
            sb.append("{\"")
                    .append(field)
                    .append("\":{\"order\":\"")
                    .append(sortItem.order == Order.ASC ? "asc" : "desc")
                    .append("\"");

            if (sortItem.nullOrder != NullOrder.UNDEFINED)
            {
                sb
                        .append(",\"missing\":\"")
                        .append(sortItem.nullOrder == NullOrder.FIRST ? "_first" : "_last")
                        .append("\"");
            }

            if (property.nestedPath != null)
            {
                if (isSingleType)
                {
                    sb
                            .append(",\"nested\":{\"path\":\"")
                            .append(property.nestedPath)
                            .append("\"}");
                }
                else
                {
                    sb
                            .append(",\"nested_path\":\"")
                            .append(property.nestedPath)
                            .append("\"");
                }
            }

            sb.append("}}")
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

    /** Meta sort items, used to build a sort query to ES */
    static class SortItemMeta
    {
        private final MappedProperty property;
        private final Order order;
        private final NullOrder nullOrder;

        SortItemMeta(MappedProperty property, Order order, NullOrder nullOrder)
        {
            this.property = requireNonNull(property, "property");
            this.order = requireNonNull(order, "order");
            this.nullOrder = requireNonNull(nullOrder, "nullOrder");
        }

        @Override
        public String toString()
        {
            String field = property.name;
            if (property.isFreeTextMapping())
            {
                MappedProperty nonFreeTextField = property.getNonFreeTextField();
                if (nonFreeTextField != null)
                {
                    field = nonFreeTextField.name;
                }
            }

            return String.format("%s %s%s", field, order, nullOrder != NullOrder.UNDEFINED ? (" missing " + nullOrder) : "");
        }
    }
}
