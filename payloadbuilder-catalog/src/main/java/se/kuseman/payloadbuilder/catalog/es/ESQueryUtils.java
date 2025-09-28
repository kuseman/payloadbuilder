package se.kuseman.payloadbuilder.catalog.es;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.hc.core5.net.URIBuilder;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.NullOrder;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.Order;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.catalog.es.ElasticsearchMetaUtils.MappedProperty;

/** Utils for ESCatalog */
final class ESQueryUtils
{
    private ESQueryUtils()
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

    /** Construct a search url */
    static String getSearchUrl(String endpoint, String index, String type, Integer size, Integer scrollMinutes, boolean isTemplate)
    {
        requireNonBlank(endpoint, "endpoint is required");

        String indexPart = isBlank(index) ? "*"
                : index;
        String typePart = useType(type) ? ("/" + type)
                : "";

        return String.format("%s/%s%s/_search%s?filter_path=_scroll_id,hits.hits&ignore_unavailable=true%s%s", endpoint, indexPart, typePart, isTemplate ? "/template"
                : "",
                scrollMinutes != null ? ("&scroll=" + scrollMinutes + "m")
                        : "",
                size != null ? ("&size=" + size)
                        : "");
    }

    private static boolean useType(String type)
    {
        return !isBlank(type)
                && !ESCatalog.SINGLE_TYPE_TABLE_NAME.equalsIgnoreCase(type);
    }

    /** Build scroll url */
    static String getScrollUrl(String endpoint, int scrollMinutes)
    {
        requireNonBlank(endpoint, "endpoint is required");
        return String.format("%s/_search/scroll?scroll=%dm&filter_path=_scroll_id,hits.hits", endpoint, scrollMinutes);
    }

    /** Build mget url */
    static String getMgetUrl(String endpoint, String index, String type)
    {
        requireNonBlank(endpoint, "endpoint is required");
        requireNonBlank(index, "index is required");

        String typePart = useType(type) ? (type + "/")
                : "";

        return String.format("%s/%s/%s_mget", endpoint, index, typePart);
    }

    /** Build search body **/
    static String getSearchBody(ElasticStrategy strategy, List<SortItemMeta> sortItems, List<IPropertyPredicate> propertyPredicates, ValueVector indexSeekValues, String indexField,
            boolean quoteIndexFieldValues, IExecutionContext context)
    {
        StringBuilder sb = new StringBuilder().append('{');
        appendSortItems(strategy, sortItems, sb);
        appendPropertyPredicates(strategy, propertyPredicates, indexSeekValues, indexField, quoteIndexFieldValues, sb, context);
        sb.append('}');
        return sb.toString();
    }

    private static void appendPropertyPredicates(ElasticStrategy strategy, List<IPropertyPredicate> propertyPredicates, ValueVector indexSeekValues, String indexField, boolean quoteIndexFieldValues,
            StringBuilder sb, IExecutionContext context)
    {
        StringBuilder filterMust = new StringBuilder();
        StringBuilder filterMustNot = new StringBuilder();
        appendIndexSearchFilter(filterMust, indexSeekValues, quoteIndexFieldValues, indexField);

        if (!propertyPredicates.isEmpty())
        {
            if (filterMust.length() > 0)
            {
                filterMust.append(",");
            }

            List<StringBuilder> sbs = asList(filterMust, filterMustNot);
            int length = sbs.size();
            for (IPropertyPredicate predicate : propertyPredicates)
            {
                predicate.appendBooleanClause(strategy, filterMust, filterMustNot, context);

                for (int i = 0; i < length; i++)
                {
                    StringBuilder sbp = sbs.get(i);
                    if (sbp.length() > 1
                            && sbp.charAt(sbp.length() - 1) != ',')
                    {
                        sbp.append(',');
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

        sb.append(',');

        appendPredicates(strategy, sb, filterMust, filterMustNot);
    }

    /** Append search body for index values */
    private static void appendIndexSearchFilter(StringBuilder sb, ValueVector indexSeekValues, boolean quoteValues, String field)
    {
        if (field == null)
        {
            return;
        }

        ValueVector value = indexSeekValues;

        if (value == null)
        {
            // No vector here, that means we have a describe/analyze-call
            // so add a dummy value
            value = ValueVector.literalString("<index values>", 1);
        }

        sb.append("{\"terms\":{\"")
                .append(field)
                .append("\":[");

        boolean first = true;
        int size = value.size();
        for (int i = 0; i < size; i++)
        {
            if (value.isNull(i))
            {
                continue;
            }
            Object val = value.valueAsObject(i);
            if (!first)
            {
                sb.append(',');
            }

            if (quoteValues)
            {
                sb.append('\"');
            }

            sb.append(val);

            if (quoteValues)
            {
                sb.append('\"');
            }
            first = false;
        }

        sb.append("]}}");
    }

    private static void appendSortItems(ElasticStrategy strategy, List<SortItemMeta> sortItems, StringBuilder sb)
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

            QualifiedName field = sortItem.property.name;
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
                    .append(field.toDotDelimited())
                    .append("\":{\"order\":\"")
                    .append(sortItem.order == Order.ASC ? "asc"
                            : "desc")
                    .append("\"");

            if (sortItem.nullOrder != NullOrder.UNDEFINED)
            {
                sb.append(",\"missing\":\"")
                        .append(sortItem.nullOrder == NullOrder.FIRST ? "_first"
                                : "_last")
                        .append('\"');
            }

            if (property.nestedPath != null)
            {
                if (strategy.wrapNestedSortPathInObject())
                {
                    sb.append(",\"nested\":{\"path\":\"")
                            .append(property.nestedPath.toDotDelimited())
                            .append("\"}");
                }
                else
                {
                    sb.append(",\"nested_path\":\"")
                            .append(property.nestedPath.toDotDelimited())
                            .append('\"');
                }
            }

            sb.append("}}")
                    .append(',');
        }
        stripLastComma(sb);
        sb.append(']');
    }

    private static void appendPredicates(ElasticStrategy strategy, StringBuilder sb, StringBuilder filterMust, StringBuilder filterMustNot)
    {
        if (strategy.supportsFilterInBoolQuery())
        {
            sb.append("\"query\":{\"bool\":{");
            if (filterMust.length() > 0)
            {
                sb.append("\"filter\":[");
                sb.append(filterMust);
                sb.append("],");
            }
        }
        else
        {
            sb.append("\"filter\":{\"bool\":{");
            if (filterMust.length() > 0)
            {
                sb.append("\"must\":[");
                sb.append(filterMust);
                sb.append("],");
            }
        }
        if (filterMustNot.length() > 0)
        {
            sb.append("\"must_not\":[");
            sb.append(filterMustNot);
            sb.append(']');
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

    /** Checks that provided string is not blank, throws exception otherwise */
    private static String requireNonBlank(String string, String message)
    {
        if (isBlank(string))
        {
            throw new IllegalArgumentException(message);
        }
        return string;
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
            QualifiedName field = property.name;
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
