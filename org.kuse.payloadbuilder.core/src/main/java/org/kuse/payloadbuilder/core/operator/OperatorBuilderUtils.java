package org.kuse.payloadbuilder.core.operator;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.parser.QualifiedReferenceExpression;

class OperatorBuilderUtils
{
    /**
     * Creates a group by operator for provided expressions
     *
     * @param nodeId Unique node id for operator
     * @param groupBys Group by expressions
     * @param operator Downstream operator to group
     */
    static Operator createGroupBy(
            int nodeId,
            List<Expression> groupBys,
            Operator operator)
    {
        // Extract column references
        // These will not return a aggregated result when queried
        // since they are the same of all rows in a group
        Map<String, QualifiedName> columnReferences = groupBys.stream()
                .filter(e -> e instanceof QualifiedReferenceExpression)
                .map(e -> (QualifiedReferenceExpression) e)
                .filter(e -> e.getQname().getParts().size() <= 2)
                .map(e -> e.getQname())
                .collect(Collectors.toMap(key -> key.getLast(), Function.identity()));

        return new GroupByOperator(
                nodeId,
                operator,
                columnReferences,
                new ExpressionValuesExtractor(groupBys),
                groupBys.size());
    }
}
