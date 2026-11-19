package se.kuseman.payloadbuilder.catalog.es;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringEscapeUtils;

import se.kuseman.payloadbuilder.api.catalog.FunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.IPredicate;
import se.kuseman.payloadbuilder.api.catalog.IPredicate.Type;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IColumnExpression;
import se.kuseman.payloadbuilder.api.expression.IComparisonExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IFunctionCallExpression;
import se.kuseman.payloadbuilder.api.expression.IInExpression;
import se.kuseman.payloadbuilder.api.expression.ILikeExpression;
import se.kuseman.payloadbuilder.api.expression.INullPredicateExpression;

/** Predicate for a property */
@SuppressWarnings("deprecation")
class PropertyPredicate implements IPropertyPredicate
{
    final String property;
    final String nestedPath;
    final IPredicate predicate;
    final boolean fullTextPredicate;

    PropertyPredicate(String property, IPredicate predicate, boolean fullTextPredicate)
    {
        this(property, null, predicate, fullTextPredicate);
    }

    PropertyPredicate(String property, String nestedPath, IPredicate predicate, boolean fullTextPredicate)
    {
        this.property = requireNonNull(property, "property");
        this.nestedPath = nestedPath;
        this.predicate = requireNonNull(predicate, "predicate");
        this.fullTextPredicate = fullTextPredicate;
    }

    @Override
    public String getDescription()
    {
        return predicate.getSqlRepresentation();
    }

    @Override
    public String getProperty()
    {
        return property;
    }

    @Override
    public IPredicate getPredicate()
    {
        return predicate;
    }

    static ValueVector evalExpression(IExpression expression, IExecutionContext context)
    {
        return expression.eval(context);
    }

    @Override
    public void appendBooleanClause(boolean describe, ElasticStrategy strategy, StringBuilder filterMust, StringBuilder filterMustNot, IExecutionContext context)
    {
        if (predicate.getType() == IPredicate.Type.COMPARISION)
        {
            ValueVector v = evalExpression(predicate.getComparisonExpression(), context);
            if (v.isNull(0))
            {
                // if we have null here this means we have a
                // query that should always return no rows
                // This because something compared to NULL using
                // a NON Null-predicate (is null, is not null) always
                // yields false so add a term that can never be true
                filterMust.append("{\"term\": { \"_id\": \"-1\" }}");

                return;
            }

            Object value = v.valueAsObject(0);
            String stringValue = quote(value);
            // CSOFF
            switch (predicate.getComparisonType())
            // CSON
            {
                case NOT_EQUAL:
                case EQUAL:
                    // CSON
                    // { "term": { "property": value }}
                    StringBuilder sb = predicate.getComparisonType() == IComparisonExpression.Type.NOT_EQUAL ? filterMustNot
                            : filterMust;
                    prependNested(sb);
                    sb.append("{\"term\":{\"")
                            .append(property)
                            .append("\":")
                            .append(stringValue)
                            .append("}}");
                    appendNested(sb);
                    return;
                case GREATER_THAN:
                case GREATER_THAN_EQUAL:
                case LESS_THAN:
                case LESS_THAN_EQUAL:

                    // { "range": { "property": { "gt": 100 }}}
                    prependNested(filterMust);
                    filterMust.append("{\"range\":{\"")
                            .append(property)
                            .append("\":{\"")
                            .append(getRangeOp(predicate.getComparisonType()))
                            .append("\":")
                            .append(stringValue)
                            .append("}}}");
                    appendNested(filterMust);
            }
        }
        else if (predicate.getType() == IPredicate.Type.IN)
        {
            IInExpression inExpression = predicate.getInExpression();
            StringBuilder sb = inExpression.isNot() ? filterMustNot
                    : filterMust;

            // { "terms": { "property": [ "value", "value2"] }}
            prependNested(sb);
            sb.append("{\"terms\":{\"")
                    .append(property)
                    .append("\":[");

            List<IExpression> arguments = inExpression.getArguments();
            sb.append(arguments.stream()
                    .map(e -> evalExpression(e, context))
                    .filter(v -> !v.isNull(0))
                    .map(o -> quote(o.valueAsObject(0)))
                    .collect(joining(",")));

            sb.append("]}}");
            appendNested(sb);
        }
        else if (predicate.getType() == IPredicate.Type.LIKE)
        {
            ILikeExpression likeExpression = predicate.getLikeExpression();
            StringBuilder sb = likeExpression.isNot() ? filterMustNot
                    : filterMust;

            Object value = evalExpression(likeExpression.getPatternExpression(), context).valueAsObject(0);
            String query = String.valueOf(value)
                    // Replace wildcard chars
                    .replace("?", "\\\\?")
                    .replace("*", "\\\\*")

                    // Replace LIKE syntax to wildcard syntax
                    .replace("%", "*")
                    .replace("_", "?");

            prependNested(sb);
            sb.append("{\"wildcard\":{\"")
                    .append(property)
                    .append("\":{\"value\":\"")
                    .append(query)
                    .append("\"")
                    .append("}}}");
            appendNested(sb);
        }
        else if (predicate.getType() == Type.NULL)
        {
            INullPredicateExpression nullExpression = predicate.getNullPredicateExpression();

            //@formatter:off
            // Elasticsearch ignore null values and these cannot be searched/filtered which means
            // that we can use exists for null predicate. If a field exists in a document that field cannot be null
            // which in turn means that if a field does not exists on a document it must be null
            // IS NULL
            //
            // "bool": {
            //    "must_not": [
            //      {
            //        "exists": {
            //          "field": <field>
            //        }
            //      }
            //    ]
            // }
            //
            // IS NOT NULL
            // "bool": {
            //    "filter": [
            //      {
            //        "exists": {
            //          "field": <field>
            //        }
            //      }
            //    ]
            // }
            //@formatter:on

            // NOTE!
            // IS NOT NULL -> must
            // IS NULL -> must_not
            StringBuilder sb = nullExpression.isNot() ? filterMust
                    : filterMustNot;

            sb.append("{\"exists\":{\"field\":\"")
                    .append(property)
                    .append("\"}}");
        }
        else if (fullTextPredicate)
        {
            appendFullTextOperator(context, predicate.getFunctionCallExpression(), filterMust);
        }
    }

    private void prependNested(StringBuilder sb)
    {
        if (nestedPath == null)
        {
            return;
        }
        // Wrap filter in nested
        // { "nested": { "path": "....", "query": {
        sb.append("{\"nested\":{\"path\":\"")
                .append(nestedPath)
                .append("\", \"query\":");
    }

    private void appendNested(StringBuilder sb)
    {
        if (nestedPath == null)
        {
            return;
        }
        sb.append("}}");
    }

    private void appendFullTextOperator(IExecutionContext context, IFunctionCallExpression functionExpression, StringBuilder sb)
    {
        FunctionInfo functionInfo = functionExpression.getFunctionInfo();
        List<IExpression> arguments = functionExpression.getArguments();

        if (MatchFunction.NAME.equals(functionInfo.getName()))
        {
            String arg0;

            IExpression arg0Expression = arguments.get(0);
            if (arg0Expression instanceof IColumnExpression)
            {
                arg0 = ((IColumnExpression) arg0Expression).getColumn();
            }
            else
            {
                arg0 = String.valueOf(evalExpression(arguments.get(0), context).valueAsObject(0));
            }

            String match = String.valueOf(evalExpression(arguments.get(1), context).valueAsObject(0));
            String[] fields = arg0.split(",");

            // TODO: - options argument
            prependNested(sb);
            if (fields.length == 1)
            {
                sb.append("{\"match\":{\"")
                        .append(fields[0])
                        .append("\":\"")
                        .append(match)
                        .append("\"")
                        .append("}}");
            }
            else
            {
                sb.append("{\"multi_match\":{")
                        .append("\"fields\":[")
                        .append(Arrays.stream(fields)
                                .map(s -> "\"" + s + "\"")
                                .collect(joining(",")))
                        .append("],\"query\":\"")
                        .append(match)
                        .append("\"")
                        .append("}}");
            }
            appendNested(sb);
        }
        else
        {
            String query = String.valueOf(evalExpression(arguments.get(0), context).valueAsObject(0));

            sb.append("{\"query_string\":{")
                    .append("\"query\":\"")
                    .append(StringEscapeUtils.escapeJson(query))
                    .append("\"")
                    .append("}}");
        }
    }

    private String getRangeOp(IComparisonExpression.Type type)
    {
        switch (type)
        {
            case GREATER_THAN:
                return "gt";
            case GREATER_THAN_EQUAL:
                return "gte";
            case LESS_THAN:
                return "lt";
            case LESS_THAN_EQUAL:
                return "lte";
            default:
                return "";
        }
    }

    static String quote(Object val)
    {
        Object value = val;
        if (value == null)
        {
            return "null";
        }

        if (value instanceof ZonedDateTime)
        {
            value = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format((Temporal) value);
        }
        else if (value instanceof LocalDateTime)
        {
            value = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(((LocalDateTime) value).atZone(ZoneId.systemDefault()));
        }

        String stringValue = String.valueOf(value);
        if (!(value instanceof Number
                || value instanceof Boolean))
        {
            return "\"" + stringValue + "\"";
        }
        return stringValue;
    }

    static boolean isSupported(IPredicate predicate, String catalogAlias)
    {
        IPredicate.Type type = predicate.getType();
        return type == Type.COMPARISION
                || type == Type.NULL
                || (type == Type.FUNCTION_CALL
                        && isFullTextSearchPredicate(predicate.getFunctionCallExpression(), catalogAlias))
                || type == Type.IN
                || (type == Type.LIKE
                        && predicate.getLikeExpression()
                                .getEscapeCharacterExpression() == null);
    }

    static boolean isFullTextSearchPredicate(IFunctionCallExpression functionExpression, String catalogAlias)
    {
        // NOT an ES catalog function
        if (!functionExpression.getCatalogAlias()
                .equalsIgnoreCase(catalogAlias))
        {
            return false;
        }
        ScalarFunctionInfo functionInfo = functionExpression.getFunctionInfo();
        String name = functionInfo.getName();
        return MatchFunction.NAME.equalsIgnoreCase(name)
                || QueryFunction.NAME.equalsIgnoreCase(name);
    }
}
