package se.kuseman.payloadbuilder.catalog.mongodb;

import static java.util.Collections.emptyList;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.bson.conversions.Bson;

import com.mongodb.client.model.Filters;

import se.kuseman.payloadbuilder.api.catalog.IPredicate;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IInExpression;
import se.kuseman.payloadbuilder.api.expression.ILikeExpression;
import se.kuseman.payloadbuilder.api.expression.INullPredicateExpression;

/**
 * Translates PLB {@link IPredicate}s into MongoDB {@link Bson} filters.
 *
 * <pre>
 * NOTE! Which predicates can be pushed down is a structural decision (column + predicate type) made once, at data source construction time via {@link #collect(List)}.
 * The actual filter values however can only be evaluated against a live {@link IExecutionContext}, so building the {@link Bson} filters themselves happens later,
 * in {@link #build(IExecutionContext, List)}, every time the data source is executed - mirroring how the Elasticsearch catalog defers its query building.
 * </pre>
 */
final class MongoFilterBuilder
{
    private MongoFilterBuilder()
    {
    }

    /** Consumes predicates from the provided (mutable) list that this catalog can push down. */
    static List<IPredicate> collect(List<IPredicate> predicates)
    {
        if (predicates.isEmpty())
        {
            return emptyList();
        }

        List<IPredicate> result = new ArrayList<>();
        Iterator<IPredicate> it = predicates.iterator();
        while (it.hasNext())
        {
            IPredicate predicate = it.next();
            if (isSupported(predicate))
            {
                result.add(predicate);
                it.remove();
            }
        }
        return result;
    }

    private static boolean isSupported(IPredicate predicate)
    {
        if (predicate.getQualifiedColumn() == null)
        {
            return false;
        }

        // CSOFF
        switch (predicate.getType())
        // CSON
        {
            case COMPARISION:
            case IN:
            case NULL:
                return true;
            case LIKE:
                // Custom escape characters aren't pushed down in v1, left for the engine
                return predicate.getLikeExpression()
                        .getEscapeCharacterExpression() == null;
            default:
                // FUNCTION_CALL/UNDEFINED (nested AND/OR etc.) are left for the engine to apply generically in v1
                return false;
        }
    }

    /** Builds Mongo filters for provided (already {@link #collect(List) collected}) predicates, evaluating their value expressions against the provided execution context. */
    static List<Bson> build(IExecutionContext context, List<IPredicate> predicates)
    {
        List<Bson> filters = new ArrayList<>(predicates.size());
        for (IPredicate predicate : predicates)
        {
            filters.add(translate(context, predicate));
        }
        return filters;
    }

    private static Bson translate(IExecutionContext context, IPredicate predicate)
    {
        String field = predicate.getQualifiedColumn()
                .toDotDelimited();

        // CSOFF
        switch (predicate.getType())
        // CSON
        {
            case COMPARISION:
                return translateComparison(context, field, predicate);
            case IN:
                return translateIn(context, field, predicate.getInExpression());
            case LIKE:
                return translateLike(context, field, predicate.getLikeExpression());
            case NULL:
                return translateNull(field, predicate.getNullPredicateExpression());
            default:
                throw new IllegalArgumentException("Unsupported predicate type: " + predicate.getType());
        }
    }

    private static Bson translateComparison(IExecutionContext context, String field, IPredicate predicate)
    {
        Object value = evalToBsonValue(context, predicate.getComparisonExpression(), field);
        if (value == null)
        {
            // A comparison against NULL never matches
            return noMatchFilter();
        }

        // CSOFF
        switch (predicate.getComparisonType())
        // CSON
        {
            case EQUAL:
                return Filters.eq(field, value);
            case NOT_EQUAL:
                return Filters.ne(field, value);
            case GREATER_THAN:
                return Filters.gt(field, value);
            case GREATER_THAN_EQUAL:
                return Filters.gte(field, value);
            case LESS_THAN:
                return Filters.lt(field, value);
            case LESS_THAN_EQUAL:
                return Filters.lte(field, value);
            default:
                throw new IllegalArgumentException("Unsupported comparison type: " + predicate.getComparisonType());
        }
    }

    private static Bson translateIn(IExecutionContext context, String field, IInExpression expression)
    {
        List<Object> values = new ArrayList<>(expression.getArguments()
                .size());
        for (IExpression arg : expression.getArguments())
        {
            Object value = evalToBsonValue(context, arg, field);
            if (value != null)
            {
                values.add(value);
            }
        }

        Bson filter = Filters.in(field, values);
        return expression.isNot() ? Filters.nor(filter)
                : filter;
    }

    private static Bson translateLike(IExecutionContext context, String field, ILikeExpression expression)
    {
        Object value = evalToBsonValue(context, expression.getPatternExpression(), field);
        if (value == null)
        {
            return noMatchFilter();
        }

        String regex = LikePatternUtils.toRegex(String.valueOf(value), null);
        Bson filter = Filters.regex(field, regex);
        return expression.isNot() ? Filters.nor(filter)
                : filter;
    }

    private static Bson translateNull(String field, INullPredicateExpression expression)
    {
        // In MongoDB {field: null} matches both an explicit null value and a missing field, which is exactly SQL's IS NULL semantics
        Bson filter = Filters.eq(field, null);
        return expression.isNot() ? Filters.nor(filter)
                : filter;
    }

    private static Bson noMatchFilter()
    {
        // A field can never both exist and not exist - a filter that never matches any document
        return Filters.and(Filters.exists("_id", true), Filters.exists("_id", false));
    }

    private static Object evalToBsonValue(IExecutionContext context, IExpression expression, String field)
    {
        ValueVector vector = expression.eval(context);
        if (vector.isNull(0))
        {
            return null;
        }
        return BsonValueConverter.toBson(vector.valueAsObject(0), field);
    }
}
