package org.kuse.payloadbuilder.catalog.es;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.operator.PredicateAnalyzer.AnalyzePair;
import org.kuse.payloadbuilder.core.operator.PredicateAnalyzer.AnalyzePair.Type;
import org.kuse.payloadbuilder.core.parser.ComparisonExpression;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.InExpression;
import org.kuse.payloadbuilder.core.parser.LikeExpression;
import org.kuse.payloadbuilder.core.parser.QualifiedFunctionCallExpression;
import org.kuse.payloadbuilder.core.parser.QualifiedReferenceExpression;

/** Predicate for a property */
class PropertyPredicate
{
    final String alias;
    final String property;
    final String nestedPath;
    final AnalyzePair pair;
    final boolean fullTextPredicate;

    PropertyPredicate(String alias, String property, AnalyzePair pair, boolean fullTextPredicate)
    {
        this(alias, property, null, pair, fullTextPredicate);
    }

    PropertyPredicate(String alias, String property, String nestedPath, AnalyzePair pair, boolean fullTextPredicate)
    {
        this.alias = alias;
        this.property = requireNonNull(property, "property");
        this.nestedPath = nestedPath;
        this.pair = requireNonNull(pair, "pair");
        this.fullTextPredicate = fullTextPredicate;
    }

    String getDescription()
    {
        if (pair.getType() == Type.COMPARISION)
        {
            Pair<Expression, Expression> ePair = pair.getExpressionPair(alias);
            return property + " " + pair.getComparisonType().toString() + " " + ePair.getRight().toString();
        }
        else if (pair.getType() == Type.IN)
        {
            return property + " IN (" + ((InExpression) pair.getRight().getExpression()).getArguments() + ")";
        }
        else if (pair.getType() == Type.LIKE)
        {
            LikeExpression le = (LikeExpression) pair.getRight().getExpression();
            return property + " LIKE " + (le.isNot() ? "NOT " : "") + le.getPatternExpression();
        }
        else if (fullTextPredicate)
        {
            return pair.getPredicate().toString();
        }

        return "";
    }

    void appendBooleanClause(
            StringBuilder filterMust,
            StringBuilder filterMustNot,

            ExecutionContext context)
    {
        if (pair.getType() == Type.COMPARISION)
        {
            Pair<Expression, Expression> ePair = pair.getExpressionPair(alias);
            Object value = ePair.getRight().eval(context);
            if (value == null)
            {
                // if we have null here this means we have a
                // query that should always return no rows
                // This because something compared to NULL using
                // a NON Null-predicate (is null, is not null) always
                // yields false so add a term that can never be true
                filterMust.append("{\"term\": { \"_id\": \"\" }}");

                return;
            }

            String stringValue = quote(value);
            //CSOFF
            switch (pair.getComparisonType())
            //CSON
            {
                case NOT_EQUAL:
                case EQUAL:
                    //CSON
                    // { "term": { "property": value }}
                    StringBuilder sb = pair.getComparisonType() == ComparisonExpression.Type.NOT_EQUAL
                        ? filterMustNot
                        : filterMust;
                    prependNested(sb);
                    sb
                            .append("{\"term\":{\"")
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
                            .append(getRangeOp(pair.getComparisonType()))
                            .append("\":")
                            .append(stringValue)
                            .append("}}}");
                    appendNested(filterMust);
            }
        }
        else if (pair.getType() == Type.IN)
        {
            // { "terms": { "property": [ "value", "value2"] }}
            InExpression ie = (InExpression) pair.getRight().getExpression();
            StringBuilder sb = ie.isNot() ? filterMustNot : filterMust;

            prependNested(sb);
            sb.append("{\"terms\":{\"")
                    .append(property)
                    .append("\":[");

            List<Expression> arguments = ie.getArguments();
            sb.append(arguments
                    .stream()
                    .map(e -> e.eval(context))
                    .filter(Objects::nonNull)
                    .map(o -> quote(o))
                    .collect(joining(",")));

            sb.append("]}}");
            appendNested(sb);
        }
        else if (pair.getType() == Type.LIKE)
        {
            LikeExpression le = (LikeExpression) pair.getRight().getExpression();
            StringBuilder sb = le.isNot() ? filterMustNot : filterMust;

            Object value = le.getPatternExpression().eval(context);
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
        else if (fullTextPredicate)
        {
            appendFullTextOperator(context, pair.getPredicate(), filterMust);
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

    private void appendFullTextOperator(ExecutionContext context, Expression expression, StringBuilder sb)
    {
        QualifiedFunctionCallExpression qfe = (QualifiedFunctionCallExpression) expression;
        ScalarFunctionInfo functionInfo = qfe.getFunctionInfo();
        List<Expression> arguments = qfe.getArguments();
        if (MatchFunction.NAME.equals(functionInfo.getName()))
        {
            String arg0;
            // Qualified reference, pick the Qname as fields
            if (arguments.get(0) instanceof QualifiedReferenceExpression)
            {
                arg0 = ((QualifiedReferenceExpression) arguments.get(0)).getQname().toDotDelimited();
            }
            else
            {
                arg0 = String.valueOf(arguments.get(0).eval(context));
            }

            String match = String.valueOf(arguments.get(1).eval(context));
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
                        .append(Arrays
                                .stream(fields)
                                .map(s -> "\"" + s + "\"")
                                .collect(joining(",")))
                        .append("],\"query\":\"")
                        .append(match)
                        .append("\"")
                        .append("}}");
            }
            appendNested(sb);

            return;
        }

        String query = String.valueOf(arguments.get(0).eval(context));

        sb.append("{\"query_string\":{")
                .append("\"query\":\"")
                .append(query)
                .append("\"")
                .append("}}");
    }

    private String getRangeOp(ComparisonExpression.Type type)
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

    private String quote(Object val)
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
        if (!(value instanceof Number || value instanceof Boolean))
        {
            return "\"" + stringValue + "\"";
        }
        return stringValue;
    }
}
