package se.kuseman.payloadbuilder.catalog.es;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.tuple.Triple;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.FunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.IPredicate;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.IQuerySession;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.AExpressionVisitor;
import se.kuseman.payloadbuilder.api.expression.IArithmeticBinaryExpression;
import se.kuseman.payloadbuilder.api.expression.IArithmeticUnaryExpression;
import se.kuseman.payloadbuilder.api.expression.IAtTimeZoneExpression;
import se.kuseman.payloadbuilder.api.expression.ICastExpression;
import se.kuseman.payloadbuilder.api.expression.IColumnExpression;
import se.kuseman.payloadbuilder.api.expression.IComparisonExpression;
import se.kuseman.payloadbuilder.api.expression.IComparisonExpression.Type;
import se.kuseman.payloadbuilder.api.expression.IDateAddExpression;
import se.kuseman.payloadbuilder.api.expression.IDateDiffExpression;
import se.kuseman.payloadbuilder.api.expression.IDatePartExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IFunctionCallExpression;
import se.kuseman.payloadbuilder.api.expression.IInExpression;
import se.kuseman.payloadbuilder.api.expression.ILikeExpression;
import se.kuseman.payloadbuilder.api.expression.ILiteralBooleanExpression;
import se.kuseman.payloadbuilder.api.expression.ILiteralDateTimeExpression;
import se.kuseman.payloadbuilder.api.expression.ILiteralDateTimeOffsetExpression;
import se.kuseman.payloadbuilder.api.expression.ILiteralDecimalExpression;
import se.kuseman.payloadbuilder.api.expression.ILiteralDoubleExpression;
import se.kuseman.payloadbuilder.api.expression.ILiteralExpression;
import se.kuseman.payloadbuilder.api.expression.ILiteralFloatExpression;
import se.kuseman.payloadbuilder.api.expression.ILiteralIntegerExpression;
import se.kuseman.payloadbuilder.api.expression.ILiteralLongExpression;
import se.kuseman.payloadbuilder.api.expression.ILiteralNullExpression;
import se.kuseman.payloadbuilder.api.expression.ILiteralStringExpression;
import se.kuseman.payloadbuilder.api.expression.ILogicalBinaryExpression;
import se.kuseman.payloadbuilder.api.expression.ILogicalNotExpression;
import se.kuseman.payloadbuilder.api.expression.INullPredicateExpression;
import se.kuseman.payloadbuilder.api.expression.ITemplateStringExpression;
import se.kuseman.payloadbuilder.api.expression.IVariableExpression;
import se.kuseman.payloadbuilder.catalog.es.ElasticsearchMetaUtils.MappedProperty;

/** Class that transpiles PLB expressions into ES counter parts. */
class ElasticQueryBuilder
{
    /** Collects and returns supported predicates from provided list. */
    static List<IPropertyPredicate> collectPredicates(IQuerySession session, List<IPredicate> predicates, String catalogAlias, Map<QualifiedName, MappedProperty> properties)
    {
        List<Triple<IPredicate, IExpression, String>> result = new ArrayList<>();
        Iterator<IPredicate> it = predicates.iterator();
        Map<QualifiedName, ResolvedProperty> resolvedProperties = null;
        while (it.hasNext())
        {
            IPredicate predicate = it.next();
            IExpression expression = predicate.getFullExpression();
            CheckResult checkResult = check(session, catalogAlias, properties, expression);
            String property = null;
            if (checkResult.supported)
            {
                if (checkResult.resolvedProperties.size() == 1)
                {
                    property = checkResult.resolvedProperties.entrySet()
                            .iterator()
                            .next()
                            .getValue().property;
                }
                if (resolvedProperties == null)
                {
                    resolvedProperties = checkResult.resolvedProperties;
                }
                else
                {
                    resolvedProperties.putAll(checkResult.resolvedProperties);
                }

                result.add(Triple.of(predicate, expression, property));
                it.remove();
            }
        }

        final Map<QualifiedName, ResolvedProperty> p = resolvedProperties;
        return result.stream()
                .map(t -> (IPropertyPredicate) new PredicateResult(t.getLeft(), t.getMiddle(), p, t.getRight()))
                .toList();
    }

    static class PredicateResult implements IPropertyPredicate
    {
        private final IPredicate predicate;
        private final IExpression expression;
        private final Map<QualifiedName, ResolvedProperty> resolvedProperties;
        private final String property;

        PredicateResult(IPredicate predicate, IExpression expression, Map<QualifiedName, ResolvedProperty> resolvedProperties, String property)
        {
            this.predicate = predicate;
            this.expression = expression;
            this.resolvedProperties = resolvedProperties;
            this.property = property;
        }

        @Override
        public String getDescription()
        {
            return predicate.getSqlRepresentation();
        }

        @Override
        public IPredicate getPredicate()
        {
            return predicate;
        }

        @Override
        public String getProperty()
        {
            return property;
        }

        @Override
        public void appendBooleanClause(ElasticStrategy strategy, StringBuilder filterMust, StringBuilder filterMustNot, IExecutionContext context)
        {
            PredicateQueryBuilderVisitor.Context ctx = new PredicateQueryBuilderVisitor.Context();
            ctx.strategy = strategy;
            ctx.resolvedProperties = resolvedProperties;
            ctx.context = context;

            IExpression e = expression;
            // If root is a not then switch top string builder and skip that expression
            ctx.filter = filterMust;
            if (e instanceof ILogicalNotExpression lne)
            {
                ctx.filter = filterMustNot;
                e = lne.getExpression();
            }
            e.accept(PredicateQueryBuilderVisitor.INSTANCE, ctx);
        }
    }

    /** Checks if provided expression is supported as a elastic predicate */
    static CheckResult check(IQuerySession session, String catalogAlias, Map<QualifiedName, MappedProperty> properties, IExpression expression)
    {
        PredicateCheckerVisitor.Context ctx = new PredicateCheckerVisitor.Context();
        ctx.properties = properties;
        ctx.catalogAlias = catalogAlias;
        ctx.session = session;
        Boolean isSupported = expression.accept(PredicateCheckerVisitor.INSTANCE, ctx);
        return new CheckResult(isSupported, ctx.result);
    }

    private record CheckResult(boolean supported, Map<QualifiedName, ResolvedProperty> resolvedProperties)
    {
    }

    /** Visitor that builds elastic query from PLB expression. */
    static class PredicateQueryBuilderVisitor extends AExpressionVisitor<Void, PredicateQueryBuilderVisitor.Context>
    {
        private static final PredicateQueryBuilderVisitor INSTANCE = new PredicateQueryBuilderVisitor();

        static class Context
        {
            ElasticStrategy strategy;
            Map<QualifiedName, ResolvedProperty> resolvedProperties;
            IExecutionContext context;
            StringBuilder filter;
            boolean inverted;

            /**
             * Appends a filter were there are no matches, this to simulate a null comparison or other constructs that is always false
             */
            void appendNoMatchFilter()
            {
                // If we are inside a not then add a match all
                if (inverted)
                {
                    // NOT (col = null)
                    filter.append("{\"match_all\":{}}");
                }
                // .. else add a match_none
                else
                {
                    // col = <null>
                    if (strategy.supportsMatchNone())
                    {
                        filter.append("{\"match_none\":{}}");
                    }
                    else
                    {
                        // Hopefully this doesn't match any docs
                        // The match_none has been present since ES 5 so I guess this is fine
                        // for those uses that old ES
                        filter.append("{\"term\": { \"_id\": \"---1\" }}");
                    }
                }
            }
        }

        @Override
        public Void visit(IComparisonExpression expression, Context context)
        {
            QualifiedName qname = expression.getLeft()
                    .getQualifiedColumn();
            IExpression valueExpression = expression.getRight();

            // Switch
            if (qname == null)
            {
                qname = expression.getRight()
                        .getQualifiedColumn();
                valueExpression = expression.getLeft();
            }
            assert (qname != null);
            assert (valueExpression != null);

            boolean not = expression.getComparisonType() == IComparisonExpression.Type.NOT_EQUAL;
            ValueVector vector = evalExpression(valueExpression, context.context);
            if (vector.isNull(0))
            {
                // if we have null here this means we have a
                // query that should always return no rows
                // This because something compared to NULL using
                // a NON Null-predicate (is null, is not null) always
                // yields false so add a term that can never be true
                context.appendNoMatchFilter();
                return null;
            }

            ResolvedProperty property = context.resolvedProperties.get(qname);
            assert (property != null);
            Object value = vector.valueAsObject(0);
            String stringValue = quote(value);
            // CSOFF
            switch (expression.getComparisonType())
            // CSON
            {
                case NOT_EQUAL:
                case EQUAL:
                    // CSON
                    // Wrap in a must_not
                    if (not)
                    {
                        context.filter.append("{\"bool\":{\"must_not\":[");
                    }

                    // { "term": { "property": value }}
                    prependNested(context.filter, property.nestedPath);
                    context.filter.append("{\"term\":{\"")
                            .append(property.property)
                            .append("\":")
                            .append(stringValue)
                            .append("}}");
                    appendNested(context.filter, property.nestedPath);

                    if (not)
                    {
                        context.filter.append("]}}");
                    }

                    return null;
                case GREATER_THAN:
                case GREATER_THAN_EQUAL:
                case LESS_THAN:
                case LESS_THAN_EQUAL:

                    // { "range": { "property": { "gt": 100 }}}
                    prependNested(context.filter, property.nestedPath);
                    context.filter.append("{\"range\":{\"")
                            .append(property.property)
                            .append("\":{\"")
                            .append(getRangeOp(expression.getComparisonType()))
                            .append("\":")
                            .append(stringValue)
                            .append("}}}");
                    appendNested(context.filter, property.nestedPath);
            }

            return null;
        }

        @Override
        public Void visit(IFunctionCallExpression expression, Context context)
        {
            appendFullTextOperator(context.context, expression, context.filter);
            return null;
        }

        @Override
        public Void visit(INullPredicateExpression expression, Context context)
        {
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

            boolean not = expression.isNot();
            if (!not)
            {
                context.filter.append("{\"bool\":{\"must_not\":[");
            }

            QualifiedName qname = expression.getExpression()
                    .getQualifiedColumn();
            ResolvedProperty property = context.resolvedProperties.get(qname);
            assert (property != null);

            prependNested(context.filter, property.nestedPath);
            context.filter.append("{\"exists\":{\"field\":\"")
                    .append(property.property)
                    .append("\"}}");
            appendNested(context.filter, property.nestedPath);

            if (!not)
            {
                context.filter.append("]}}");
            }
            return null;
        }

        @Override
        public Void visit(ILikeExpression expression, Context context)
        {
            QualifiedName qname = expression.getExpression()
                    .getQualifiedColumn();
            ResolvedProperty property = context.resolvedProperties.get(qname);
            assert (property != null);

            Object value = evalExpression(expression.getPatternExpression(), context.context).valueAsObject(0);
            if (value == null)
            {
                context.appendNoMatchFilter();
                return null;
            }

            boolean not = expression.isNot();
            if (not)
            {
                context.filter.append("{\"bool\":{\"must_not\":[");
            }

            String query = String.valueOf(value)
                    // Replace wildcard chars
                    .replace("?", "\\\\?")
                    .replace("*", "\\\\*")

                    // Replace LIKE syntax to wildcard syntax
                    .replace("%", "*")
                    .replace("_", "?");

            prependNested(context.filter, property.nestedPath);
            context.filter.append("{\"wildcard\":{\"")
                    .append(property.property)
                    .append("\":{\"value\":\"")
                    .append(query)
                    .append("\"")
                    .append("}}}");
            appendNested(context.filter, property.nestedPath);
            if (not)
            {
                context.filter.append("]}}");
            }
            return null;
        }

        @Override
        public Void visit(IInExpression expression, Context context)
        {
            boolean not = expression.isNot();
            if (not)
            {
                context.filter.append("{\"bool\":{\"must_not\":[");
            }

            QualifiedName qname = expression.getExpression()
                    .getQualifiedColumn();

            // <col> IN (<literal/variable>)
            if (qname != null)
            {
                ResolvedProperty property = context.resolvedProperties.get(qname);
                assert (property != null);

                List<IExpression> arguments = expression.getArguments();
                String args = arguments.stream()
                        .map(e -> evalExpression(e, context.context))
                        .filter(v -> !v.isNull(0))
                        .map(o -> quote(o.valueAsObject(0)))
                        .collect(joining(","));

                // TODO: all null

                // { "terms": { "property": [ "value", "value2"] }}
                prependNested(context.filter, property.nestedPath);
                context.filter.append("{\"terms\":{\"")
                        .append(property.property)
                        .append("\":[")
                        .append(args)
                        .append("]}}");
                appendNested(context.filter, property.nestedPath);

                if (not)
                {
                    context.filter.append("]}}");
                }

                return null;
            }

            // <literal/variable> IN (col1, col2)
            ValueVector vv = evalExpression(expression.getExpression(), context.context);
            if (vv.isNull(0))
            {
                context.appendNoMatchFilter();
                return null;
            }

            String value = quote(vv.valueAsObject(0));
            context.filter.append("{\"bool\":{\"should\":[");
            boolean first = true;
            for (IExpression arg : expression.getArguments())
            {
                qname = arg.getQualifiedColumn();
                assert (qname != null);
                ResolvedProperty property = context.resolvedProperties.get(qname);
                assert (property != null);

                if (!first)
                {
                    context.filter.append(',');
                }

                prependNested(context.filter, property.nestedPath);
                context.filter.append("{\"term\":{\"%s\":%s}}".formatted(property.property, value));
                appendNested(context.filter, property.nestedPath);
                first = false;
            }
            context.filter.append("]}}");

            if (not)
            {
                context.filter.append("]}}");
            }
            return null;
        }

        @Override
        public Void visit(ILogicalBinaryExpression expression, Context context)
        {
            // (a.col = 1 AND b.col = 2)
            /*
             * "bool": { "filter/should": [ ... append left + right ] }
             */

            String andVariant = context.strategy.supportsFilterInBoolQuery() ? "filter"
                    : "must";

            context.filter.append("{\"bool\":{\"")
                    .append(expression.getLogicalType() == ILogicalBinaryExpression.Type.AND ? andVariant
                            : "should")
                    .append("\":[");

            expression.getLeft()
                    .accept(this, context);
            context.filter.append(',');
            expression.getRight()
                    .accept(this, context);
            context.filter.append("]}}");

            return null;
        }

        @Override
        public Void visit(ILogicalNotExpression expression, Context context)
        {
            context.inverted = !context.inverted;

            // Wrap in a must_not
            context.filter.append("{\"bool\":{\"must_not\":[");

            expression.getExpression()
                    .accept(this, context);

            context.filter.append("]}}");

            context.inverted = !context.inverted;
            return null;
        }

        private void prependNested(StringBuilder sb, String nestedPath)
        {
            if (isBlank(nestedPath))
            {
                return;
            }
            // Wrap filter in nested
            // { "nested": { "path": "....", "query": {
            sb.append("{\"nested\":{\"path\":\"")
                    .append(nestedPath)
                    .append("\", \"query\":");
        }

        private void appendNested(StringBuilder sb, String nestedPath)
        {
            if (isBlank(nestedPath))
            {
                return;
            }
            sb.append("}}");
        }

        private String quote(Object val)
        {
            Object value = val;
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

        private String getRangeOp(IComparisonExpression.Type type)
        {
            return switch (type)
            {
                case GREATER_THAN -> "gt";
                case GREATER_THAN_EQUAL -> "gte";
                case LESS_THAN -> "lt";
                case LESS_THAN_EQUAL -> "lte";
                default -> "";
            };
        }

        @SuppressWarnings("deprecation")
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
                // prependNested(sb);
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
                // appendNested(sb);
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

        private ValueVector evalExpression(IExpression expression, IExecutionContext context)
        {
            return expression.eval(context);
        }
    }

    /** Visitor that checks if an expression is supported as a Elastic query. */
    static class PredicateCheckerVisitor extends AExpressionVisitor<Boolean, PredicateCheckerVisitor.Context>
    {
        static final PredicateCheckerVisitor INSTANCE = new PredicateCheckerVisitor();
        //@formatter:off
        static final Predicate<IExpression> IS_LITERAL_OR_VARIABLE = e -> 
                e instanceof IVariableExpression 
             || e instanceof ILiteralExpression;
        //@formatter:on

        static class Context
        {
            IQuerySession session;
            String catalogAlias;
            Map<QualifiedName, ResolvedProperty> result = new HashMap<>();
            Map<QualifiedName, MappedProperty> properties;
            int nestLevel;
        }

        @Override
        protected Boolean defaultResult(Context context)
        {
            // Default for all expressions is that we don't support anything.
            return false;
        }

        @Override
        protected Boolean aggregate(Boolean result, Boolean nextResult)
        {
            return result
                    && nextResult;
        }

        @Override
        public Boolean visit(ILiteralBooleanExpression expression, Context context)
        {
            return true;
        }

        @Override
        public Boolean visit(ILiteralNullExpression expression, Context context)
        {
            return true;
        }

        @Override
        public Boolean visit(ILiteralFloatExpression expression, Context context)
        {
            return true;
        }

        @Override
        public Boolean visit(ILiteralLongExpression expression, Context context)
        {
            return true;
        }

        @Override
        public Boolean visit(ILiteralDecimalExpression expression, Context context)
        {
            return true;
        }

        @Override
        public Boolean visit(ILiteralIntegerExpression expression, Context context)
        {
            return true;
        }

        @Override
        public Boolean visit(ILiteralDoubleExpression expression, Context context)
        {
            return true;
        }

        @Override
        public Boolean visit(ILiteralStringExpression expression, Context context)
        {
            return true;
        }

        @Override
        public Boolean visit(ILiteralDateTimeExpression expression, Context context)
        {
            return true;
        }

        @Override
        public Boolean visit(ILiteralDateTimeOffsetExpression expression, Context context)
        {
            return true;
        }

        @Override
        public Boolean visit(ITemplateStringExpression expression, Context context)
        {
            return expression.getExpressions()
                    .stream()
                    .allMatch(e -> e.accept(this, context));
        }

        @Override
        public Boolean visit(IDateAddExpression expression, Context context)
        {
            return expression.getExpression()
                    .accept(this, context);
        }

        @Override
        public Boolean visit(IDateDiffExpression expression, Context context)
        {
            return expression.getStart()
                    .accept(this, context)
                    && expression.getEnd()
                            .accept(this, context);
        }

        @Override
        public Boolean visit(IDatePartExpression expression, Context context)
        {
            return expression.getExpression()
                    .accept(this, context);
        }

        @Override
        public Boolean visit(IAtTimeZoneExpression expression, Context context)
        {
            return expression.getExpression()
                    .accept(this, context);
        }

        @Override
        public Boolean visit(ICastExpression expression, Context context)
        {
            return expression.getExpression()
                    .accept(this, context);
        }

        @Override
        public Boolean visit(IVariableExpression expression, Context context)
        {
            return true;
        }

        @Override
        public Boolean visit(IFunctionCallExpression expression, Context context)
        {
            // On root level we only support our own ES functions
            if (context.nestLevel == 0)
            {
                return isFullTextSearchPredicate(expression, context.catalogAlias);
            }

            // ... else we support our own plus any other function that has supported arguments
            return isFullTextSearchPredicate(expression, context.catalogAlias)
                    || expression.getArguments()
                            .stream()
                            .allMatch(e -> e.accept(this, context));
        }

        @Override
        public Boolean visit(ILogicalNotExpression expression, Context context)
        {
            context.nestLevel++;
            Boolean result = expression.getExpression()
                    .accept(this, context);
            context.nestLevel--;
            return result;
        }

        @Override
        public Boolean visit(ILogicalBinaryExpression expression, Context context)
        {
            context.nestLevel++;
            Boolean result = expression.getLeft()
                    .accept(this, context)
                    && expression.getRight()
                            .accept(this, context);
            context.nestLevel--;
            return result;
        }

        @Override
        public Boolean visit(IArithmeticBinaryExpression expression, Context context)
        {
            context.nestLevel++;
            Boolean result = expression.getLeft()
                    .accept(this, context)
                    && expression.getRight()
                            .accept(this, context);
            context.nestLevel--;
            return result;
        }

        @Override
        public Boolean visit(IArithmeticUnaryExpression expression, Context context)
        {
            context.nestLevel++;
            Boolean result = expression.getExpression()
                    .accept(this, context);
            context.nestLevel--;
            return result;
        }

        @Override
        public Boolean visit(IComparisonExpression expression, Context context)
        {
            Type type = expression.getComparisonType();
            QualifiedName leftQname = expression.getLeft()
                    .getQualifiedColumn();
            QualifiedName rightQname = expression.getRight()
                    .getQualifiedColumn();

            // This expression is not directly referencing columns, nested and/ors etc.
            // This is not supported because there could be an expression like this:
            // (col > 10) = (col2 > 20)
            // ... which is a perfectly valid expression but we cannot transform such comparison to an ES query
            // A comparison expression must have one and only one qualifier
            if (leftQname == null
                    && rightQname == null)
            {
                return false;
            }
            // Elasticsearch doesn't support expressions of type "col1 <any comparison op> col2"
            else if (leftQname != null
                    && rightQname != null)
            {
                return false;
            }

            boolean isEqualOrNonEqual = type == IComparisonExpression.Type.EQUAL
                    || type == IComparisonExpression.Type.NOT_EQUAL;

            QualifiedName qname = leftQname;
            IExpression other = expression.getRight();
            if (qname == null)
            {
                qname = rightQname;
                other = expression.getLeft();
            }

            context.nestLevel++;
            // Check other side
            if (!other.accept(this, context))
            {
                context.nestLevel--;
                return false;
            }
            context.nestLevel--;

            if (isSpecialQualifier(qname))
            {
                // Extra columns only support EQUALS/NOT_EQUALS
                if (!isEqualOrNonEqual)
                {
                    return false;
                }

                context.result.put(qname, new ResolvedProperty(getSpecialQualifierEsColumn(qname), ""));
                return true;
            }

            MappedProperty property = getMappedProperty(context.properties, qname);
            if (property == null)
            {
                return false;
            }

            // Strings only supports equal/not equal
            if (property.isStringType()
                    && !isEqualOrNonEqual)
            {
                return false;
            }

            // We cannot do comparisons on free text mappings, see if there is a field
            if (property.isFreeTextMapping())
            {
                property = property.getNonFreeTextField();
                if (property == null)
                {
                    return false;
                }
            }

            context.result.put(qname, new ResolvedProperty(property.name.toDotDelimited(), defaultIfNull(property.nestedPath, QualifiedName.EMPTY).toDotDelimited()));
            return true;
        }

        @Override
        public Boolean visit(ILikeExpression expression, Context context)
        {
            if (!context.session.getCatalogProperty(context.catalogAlias, ESCatalog.USE_LIKE_EXPRESSION, ValueVector.literalBoolean(true, 1))
                    .getBoolean(0))
            {
                return false;
            }

            // TODO: wildcard is only supported on keyword/text
            QualifiedName qname = expression.getExpression()
                    .getQualifiedColumn();
            return expression.getEscapeCharacterExpression() == null
                    && putQualifier(qname, context, true);
        }

        @Override
        public Boolean visit(INullPredicateExpression expression, Context context)
        {
            QualifiedName qname = expression.getExpression()
                    .getQualifiedColumn();
            return putQualifier(qname, context, false);
        }

        @Override
        public Boolean visit(IInExpression expression, Context context)
        {
            // We support in expressions in the forms:
            // - <col> IN (<literal/variable>, <literal/variable>) => a terms query
            // - <literal/variable> IN (<col1>, <col2>) => should query (NOT (a = 10 OR b = 10))
            // .. where all columns are mapped

            if (IS_LITERAL_OR_VARIABLE.test(expression.getExpression()))
            {
                for (IExpression arg : expression.getArguments())
                {
                    QualifiedName qname = arg.getQualifiedColumn();
                    // If we encounter a non qualified arg drop out
                    if (!putQualifier(qname, context, true))
                    {
                        return false;
                    }
                }
                return true;
            }

            // <col> IN (<literal/variable>)
            QualifiedName qname = expression.getExpression()
                    .getQualifiedColumn();
            return expression.getArguments()
                    .stream()
                    .allMatch(IS_LITERAL_OR_VARIABLE)
                    && putQualifier(qname, context, true);
        }

        private boolean putQualifier(QualifiedName qname, Context context, boolean checkNonFreeTextField)
        {
            if (qname == null)
            {
                return false;
            }

            // Built in columns
            if (isSpecialQualifier(qname))
            {
                context.result.put(qname, new ResolvedProperty(getSpecialQualifierEsColumn(qname), ""));
                return true;
            }

            MappedProperty property = getMappedProperty(context.properties, qname);
            if (property == null)
            {
                return false;
            }
            if (checkNonFreeTextField
                    && property.isFreeTextMapping())
            {
                property = property.getNonFreeTextField();
                if (property == null)
                {
                    return false;
                }
            }
            context.result.put(qname, new ResolvedProperty(property.name.toDotDelimited(), defaultIfNull(property.nestedPath, QualifiedName.EMPTY).toDotDelimited()));
            return true;
        }

        private boolean isSpecialQualifier(QualifiedName qname)
        {
            return ESDatasource.INDEX.equalsIgnoreCase(qname)
                    || ESDatasource.TYPE.equalsIgnoreCase(qname)
                    || ESDatasource.DOCID.equalsIgnoreCase(qname);
        }

        private String getSpecialQualifierEsColumn(QualifiedName qname)
        {
            if (ESDatasource.INDEX.equalsIgnoreCase(qname))
            {
                return "_index";
            }
            else if (ESDatasource.TYPE.equalsIgnoreCase(qname))
            {
                return "_type";
            }
            return "_id";
        }

        private boolean isFullTextSearchPredicate(IFunctionCallExpression functionExpression, String catalogAlias)
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

    private static MappedProperty getMappedProperty(Map<QualifiedName, MappedProperty> properties, QualifiedName qname)
    {
        requireNonNull(qname);
        qname = qname.toLowerCase();
        MappedProperty property = properties.get(qname);

        // If we did not find any column and we have single qname with a dot in it, it's highly likely
        // that the column we are searching for is a multi qname to transform it and try
        if (property == null
                && qname.size() == 1
                && qname.getFirst()
                        .contains("."))
        {
            qname = QualifiedName.of(qname.getFirst()
                    .split("\\."));
            property = properties.get(qname);
        }

        return property;
    }

    /** Resolved property for a qualified name found in predicate expression. */
    record ResolvedProperty(String property, String nestedPath)
    {
    }
}
