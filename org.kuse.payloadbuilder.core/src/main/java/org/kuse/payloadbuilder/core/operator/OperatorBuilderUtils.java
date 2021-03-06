package org.kuse.payloadbuilder.core.operator;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.QualifiedReferenceExpression;
import org.kuse.payloadbuilder.core.parser.QualifiedReferenceExpression.ResolvePath;

/** Utils for operator building */
class OperatorBuilderUtils
{
    /**
     * Creates a group by operator for provided expressions
     */
    static Operator createGroupBy(
            int nodeId,
            List<Expression> groupBys,
            IIndexValuesFactory indexValuesFactory,
            Operator operator)
    {
        // Extract column references
        // These will not return a aggregated result when queried
        // since they are the same of all rows in a group
        Map<Integer, Set<String>> columnReferences = groupBys.stream()
                .filter(e -> e instanceof QualifiedReferenceExpression)
                .map(e -> (QualifiedReferenceExpression) e)
                .filter(e -> e.getQname().getParts().size() <= 2)
                .map(e ->
                {
                    @SuppressWarnings("deprecation")
                    ResolvePath[] paths = e.getResolvePaths();
                    if (paths == null)
                    {
                        return null;
                    }

                    return Pair.of(paths[0].getTargetTupleOrdinal(), e.getQname().getLast());
                })
                .filter(Objects::nonNull)
                .collect(groupingBy(Pair::getKey, HashMap::new, mapping(Pair::getValue, toSet())));

        return new GroupByOperator(
                nodeId,
                operator,
                columnReferences,
                indexValuesFactory,
                groupBys.size());
    }
}
