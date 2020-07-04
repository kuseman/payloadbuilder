package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.parser.Expression;
import com.viskan.payloadbuilder.parser.QualifiedReferenceExpression;

import static java.util.stream.Collectors.toList;

import java.util.List;

class OperatorBuilderUtils
{
    
    
    /** Creates a group by operator for provided expressions 
     * @param nodeId Unique node id for operator
     * @param alias Alias of the belonging table source
     * @param groupBys Group by expressions
     * @param operator Downstream operator to group
     * */
    static Operator createGroupBy(
            int nodeId,
            String alias,
            List<Expression> groupBys,
            Operator operator)
    {
        // Extract column references
        // These will not return a aggregated result when queried
        // since they are the same of all rows in a group
        List<String> columnReferences = groupBys.stream()
                .filter(e -> e instanceof QualifiedReferenceExpression)
                .map(e -> (QualifiedReferenceExpression) e)
                .filter(e -> e.getQname().getParts().size() == 2 && e.getQname().getAlias().equals(alias))
                .map(e -> e.getQname().getLast())
                .collect(toList());

        return new GroupByOperator(
                nodeId,
                operator,
                columnReferences,
                new ExpressionValuesExtractor(groupBys),
                groupBys.size());
    }
}
