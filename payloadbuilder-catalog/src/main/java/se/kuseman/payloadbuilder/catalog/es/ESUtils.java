package se.kuseman.payloadbuilder.catalog.es;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.List;

import org.apache.http.client.utils.URIBuilder;

import se.kuseman.payloadbuilder.api.catalog.ISortItem.NullOrder;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.Order;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.api.operator.IIndexPredicate;
import se.kuseman.payloadbuilder.api.operator.IOrdinalValues;
import se.kuseman.payloadbuilder.api.session.IQuerySession;
import se.kuseman.payloadbuilder.api.utils.StringUtils;
import se.kuseman.payloadbuilder.catalog.es.ESCatalog.MappedProperty;

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
            return builder.build()
                    .toURL()
                    .toExternalForm();
        }
        catch (MalformedURLException | URISyntaxException e)
        {
            throw new RuntimeException("Error creating URL", e);
        }
    }

    /** Get use doc type property from query session */
    static boolean getUseDocType(IQuerySession session, String catalogAlias)
    {
        Boolean useDocType = session.getCatalogProperty(catalogAlias, ESCatalog.USE_DOC_TYPE_KEY);
        return useDocType == null ? false
                : useDocType;
    }

    /** Build search url */
    static String getSearchUrl(boolean useDocType, String endpoint, String index, String type, Integer size, Integer scrollMinutes, String indexField)
    {
        if (ESCatalog.SINGLE_TYPE_TABLE_NAME.equals(type)
                && !useDocType)
        {
            type = "";
        }

        StringUtils.requireNonBlank(endpoint, "endpoint is required");
        return String.format("%s/%s%s/_search?filter_path=_scroll_id,hits.hits%s%s", endpoint, isBlank(index) ? "*"
                : index,
                isBlank(type) ? ""
                        : ("/" + type),
                scrollMinutes != null ? ("&scroll=" + scrollMinutes + "m")
                        : "",
                size != null ? ("&size=" + size)
                        : "");
    }

    /** Build scroll url */
    static String getScrollUrl(String endpoint, int scrollMinutes)
    {
        StringUtils.requireNonBlank(endpoint, "endpoint is required");
        return String.format("%s/_search/scroll?scroll=%dm&filter_path=_scroll_id,hits.hits", endpoint, scrollMinutes);
    }

    /** Build search template url */
    static String getSearchTemplateUrl(boolean useDocType, String endpoint, String index, String type, Integer size, Integer scrollMinutes)
    {
        if (ESCatalog.SINGLE_TYPE_TABLE_NAME.equals(type)
                && !useDocType)
        {
            type = "";
        }

        StringUtils.requireNonBlank(endpoint, "endpoint is required");
        return String.format("%s/%s/%s_search/template?filter_path=_scroll_id,hits.hits%s%s", endpoint, isBlank(index) ? "*"
                : index,
                isBlank(type) ? ""
                        : (type + "/"),
                scrollMinutes != null ? ("&scroll=" + scrollMinutes + "m")
                        : "",
                size != null ? ("&size=" + size)
                        : "");
    }

    /** Build mget url */
    static String getMgetUrl(boolean useDocType, String endpoint, String index, String type)
    {
        if (ESCatalog.SINGLE_TYPE_TABLE_NAME.equals(type)
                && !useDocType)
        {
            type = "";
        }
        else
        {
            StringUtils.requireNonBlank(type, "type is required");
        }

        StringUtils.requireNonBlank(endpoint, "endpoint is required");
        StringUtils.requireNonBlank(index, "index is required");
        return String.format("%s/%s/%s_mget", endpoint, index, !isBlank(type) ? (type + "/")
                : "");
    }

    /** Build search body **/
    static String getSearchBody(List<SortItemMeta> sortItems, List<PropertyPredicate> propertyPredicates, IIndexPredicate indexPredicate, String indexField, boolean quoteIndexFieldValues,
            boolean isSingleType, IExecutionContext context)
    {
        StringBuilder sb = new StringBuilder("{");
        appendSortItems(sortItems, sb, isSingleType);
        appendPropertyPredicates(propertyPredicates, indexPredicate, indexField, quoteIndexFieldValues, isSingleType, sb, context);
        sb.append("}");
        return sb.toString();
    }

    private static void appendPropertyPredicates(List<PropertyPredicate> propertyPredicates, IIndexPredicate indexPredicate, String indexField, boolean quoteIndexFieldValues, boolean isSingleType,
            StringBuilder sb, IExecutionContext context)
    {
        StringBuilder filterMust = new StringBuilder();
        StringBuilder filterMustNot = new StringBuilder();
        appendIndexSearchFilter(filterMust, indexPredicate, context, quoteIndexFieldValues, indexField);

        if (!propertyPredicates.isEmpty())
        {
            if (filterMust.length() > 0)
            {
                filterMust.append(",");
            }

            List<StringBuilder> sbs = asList(filterMust, filterMustNot);
            int length = sbs.size();
            for (PropertyPredicate predicate : propertyPredicates)
            {
                predicate.appendBooleanClause(filterMust, filterMustNot, context);

                for (int i = 0; i < length; i++)
                {
                    StringBuilder sbp = sbs.get(i);
                    if (sbp.length() > 1
                            && sbp.charAt(sbp.length() - 1) != ',')
                    {
                        sbp.append(",");
                    }
                }
            }

            for (int i = 0; i < length; i++)
            {
                stripLastComma(sbs.get(i));
            }
        }

        if (filterMust.length() == 0
                && filterMustNot.length() == 0)
        {
            return;
        }

        sb.append(",");

        if (isSingleType)
        {
            appendSingleTypePredicates(sb, filterMust, filterMustNot);
        }
        else
        {
            appendNonSingleTypePredicate(sb, filterMust, filterMustNot);
        }
    }

    /** Append search body for index values */
    private static void appendIndexSearchFilter(StringBuilder sb, IIndexPredicate indexPredicate, IExecutionContext context, boolean quoteValues, String field)
    {
        if (field == null)
        {
            return;
        }

        Iterator<IOrdinalValues> it = indexPredicate != null ? indexPredicate.getOuterValuesIterator(context)
                : null;

        // No iterator here, that means we have a describe/analyze-call
        // so add a dummy value
        if (it == null)
        {
            IOrdinalValues dummy = new IOrdinalValues()
            {
                @Override
                public int size()
                {
                    return 1;
                }

                @Override
                public Object getValue(int ordinal)
                {
                    return "<index values>";
                }
            };

            it = asList(dummy).iterator();
        }

        if (!it.hasNext())
        {
            return;
        }

        sb.append("{\"terms\":{\"")
                .append(field)
                .append("\":[");

        boolean first = true;
        while (it.hasNext())
        {
            Object val = it.next()
                    .getValue(0);
            if (val == null)
            {
                continue;
            }
            if (!first)
            {
                sb.append(",");
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
            first = false;
        }

        sb.append("]}}");
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
                    .append(sortItem.order == Order.ASC ? "asc"
                            : "desc")
                    .append("\"");

            if (sortItem.nullOrder != NullOrder.UNDEFINED)
            {
                sb.append(",\"missing\":\"")
                        .append(sortItem.nullOrder == NullOrder.FIRST ? "_first"
                                : "_last")
                        .append("\"");
            }

            if (property.nestedPath != null)
            {
                if (isSingleType)
                {
                    sb.append(",\"nested\":{\"path\":\"")
                            .append(property.nestedPath)
                            .append("\"}");
                }
                else
                {
                    sb.append(",\"nested_path\":\"")
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

    // CSOFF
    private static void appendNonSingleTypePredicate(
            // CSON
            StringBuilder sb, StringBuilder filterMust, StringBuilder filterMustNot)
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
            // CSOFF
            if (addComma)
            // CSON
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
    private static void appendSingleTypePredicates(StringBuilder sb, StringBuilder filterMust, StringBuilder filterMustNot)
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
        if (sb.length() >= 1
                && sb.charAt(sb.length() - 1) == ',')
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

            return String.format("%s %s%s", field, order, nullOrder != NullOrder.UNDEFINED ? (" missing " + nullOrder)
                    : "");
        }
    }
}
