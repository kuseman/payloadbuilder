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

    // Partition filtering. Every predicate is ANDed together, so this holds one entry per predicate (an EQUAL
    // predicate contributes a single-value entry, an IN predicate contributes its full argument list) and the
    // final set of target partitions is the INTERSECTION of these entries, not their union.
    List<List<IExpression>> partitionFilter;

    // Offset range. Every predicate is ANDed together, so all lower bounds must hold simultaneously (the
    // strongest/highest wins) and likewise all upper bounds (the strongest/lowest wins).
    List<Bound> offsetLower;
    List<Bound> offsetUpper;

    // Timestamp range, combined the same way as the offset range above.
    List<Bound> timestampLower;
    List<Bound> timestampUpper;

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
        List<IExpression> values;
        if (predicate.getType() == IPredicate.Type.COMPARISION
                && predicate.getComparisonType() == IComparisonExpression.Type.EQUAL)
        {
            values = List.of(predicate.getComparisonExpression());
        }
        else if (predicate.getType() == IPredicate.Type.IN)
        {
            values = predicate.getInExpression()
                    .getArguments();
        }
        else
        {
            return false;
        }

        if (analysis.partitionFilter == null)
        {
            analysis.partitionFilter = new ArrayList<>();
        }
        analysis.partitionFilter.add(values);
        return true;
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
                addLower(analysis, isOffset, new Bound(expr, true));
                addUpper(analysis, isOffset, new Bound(expr, true));
                return true;

            case GREATER_THAN:
                addLower(analysis, isOffset, new Bound(expr, false));
                return true;

            case GREATER_THAN_EQUAL:
                addLower(analysis, isOffset, new Bound(expr, true));
                return true;

            case LESS_THAN:
                addUpper(analysis, isOffset, new Bound(expr, false));
                return true;

            case LESS_THAN_EQUAL:
                addUpper(analysis, isOffset, new Bound(expr, true));
                return true;

            default:
                return false;
        }
    }

    private static void addLower(KafkaPredicateAnalysis analysis, boolean isOffset, Bound bound)
    {
        if (isOffset)
        {
            if (analysis.offsetLower == null)
            {
                analysis.offsetLower = new ArrayList<>();
            }
            analysis.offsetLower.add(bound);
        }
        else
        {
            if (analysis.timestampLower == null)
            {
                analysis.timestampLower = new ArrayList<>();
            }
            analysis.timestampLower.add(bound);
        }
    }

    private static void addUpper(KafkaPredicateAnalysis analysis, boolean isOffset, Bound bound)
    {
        if (isOffset)
        {
            if (analysis.offsetUpper == null)
            {
                analysis.offsetUpper = new ArrayList<>();
            }
            analysis.offsetUpper.add(bound);
        }
        else
        {
            if (analysis.timestampUpper == null)
            {
                analysis.timestampUpper = new ArrayList<>();
            }
            analysis.timestampUpper.add(bound);
        }
    }
}
