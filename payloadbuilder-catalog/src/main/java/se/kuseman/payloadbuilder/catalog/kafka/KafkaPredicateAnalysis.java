package se.kuseman.payloadbuilder.catalog.kafka;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.IPredicate;
import se.kuseman.payloadbuilder.api.expression.IComparisonExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Captures predicate pushdown decisions for Kafka datasources */
class KafkaPredicateAnalysis
{
    private static final QualifiedName PARTITION = QualifiedName.of("partition");
    private static final QualifiedName OFFSET = QualifiedName.of("offset");
    private static final QualifiedName TIMESTAMP = QualifiedName.of("timestamp");

    /** A bound expression with inclusivity (used for range predicates) */
    record Bound(IExpression expression, boolean inclusive)
    {
    }

    // Partition filtering
    List<IExpression> partitionFilter;

    // Offset range [lower, upper)
    Bound offsetLower;
    Bound offsetUpper;

    // Timestamp range [lower, upper)
    Bound timestampLower;
    Bound timestampUpper;

    /** Analyze predicates, extracting pushdown-able ones and removing them from the list */
    static KafkaPredicateAnalysis analyze(List<IPredicate> predicates)
    {
        KafkaPredicateAnalysis analysis = new KafkaPredicateAnalysis();

        if (predicates == null
                || predicates.isEmpty())
        {
            return analysis;
        }

        Iterator<IPredicate> it = predicates.iterator();
        while (it.hasNext())
        {
            IPredicate predicate = it.next();
            QualifiedName column = predicate.getQualifiedColumn();
            if (column == null)
            {
                continue;
            }

            if (column.equals(PARTITION))
            {
                if (extractPartition(predicate, analysis))
                {
                    it.remove();
                }
            }
            else if (column.equals(OFFSET))
            {
                if (extractRange(predicate, analysis, true))
                {
                    it.remove();
                }
            }
            else if (column.equals(TIMESTAMP))
            {
                if (extractRange(predicate, analysis, false))
                {
                    it.remove();
                }
            }
        }

        return analysis;
    }

    private static boolean extractPartition(IPredicate predicate, KafkaPredicateAnalysis analysis)
    {
        if (predicate.getType() == IPredicate.Type.COMPARISION
                && predicate.getComparisonType() == IComparisonExpression.Type.EQUAL)
        {
            if (analysis.partitionFilter == null)
            {
                analysis.partitionFilter = new ArrayList<>();
            }
            analysis.partitionFilter.add(predicate.getComparisonExpression());
            return true;
        }

        if (predicate.getType() == IPredicate.Type.IN)
        {
            if (analysis.partitionFilter == null)
            {
                analysis.partitionFilter = new ArrayList<>();
            }
            analysis.partitionFilter.addAll(predicate.getInExpression()
                    .getArguments());
            return true;
        }

        return false;
    }

    private static boolean extractRange(IPredicate predicate, KafkaPredicateAnalysis analysis, boolean isOffset)
    {
        if (predicate.getType() != IPredicate.Type.COMPARISION)
        {
            return false;
        }

        IExpression expr = predicate.getComparisonExpression();
        IComparisonExpression.Type cmpType = predicate.getComparisonType();

        // CSOFF
        switch (cmpType)
        // CSON
        {
            case EQUAL:
                // offset = N -> lower inclusive N, upper inclusive N
                setLower(analysis, isOffset, new Bound(expr, true));
                setUpper(analysis, isOffset, new Bound(expr, true));
                return true;

            case GREATER_THAN:
                setLower(analysis, isOffset, new Bound(expr, false));
                return true;

            case GREATER_THAN_EQUAL:
                setLower(analysis, isOffset, new Bound(expr, true));
                return true;

            case LESS_THAN:
                setUpper(analysis, isOffset, new Bound(expr, false));
                return true;

            case LESS_THAN_EQUAL:
                setUpper(analysis, isOffset, new Bound(expr, true));
                return true;

            default:
                return false;
        }
    }

    private static void setLower(KafkaPredicateAnalysis analysis, boolean isOffset, Bound bound)
    {
        if (isOffset)
        {
            analysis.offsetLower = bound;
        }
        else
        {
            analysis.timestampLower = bound;
        }
    }

    private static void setUpper(KafkaPredicateAnalysis analysis, boolean isOffset, Bound bound)
    {
        if (isOffset)
        {
            analysis.offsetUpper = bound;
        }
        else
        {
            analysis.timestampUpper = bound;
        }
    }
}
