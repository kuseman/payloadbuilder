/**
 *
 *  Copyright (c) Marcus Henriksson <kuseman80@gmail.com>
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package org.kuse.payloadbuilder.core.operator;

import static java.util.stream.Collectors.toSet;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

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
        Set<QualifiedName> columnReferences = groupBys.stream()
                .filter(e -> e instanceof QualifiedReferenceExpression)
                .map(e -> (QualifiedReferenceExpression) e)
                .filter(e -> e.getQname().getParts().size() <= 2)
                .map(e -> e.getQname())
                .flatMap(q ->
                {
                    // Split up 2 part qnames to full and column only
                    // to be able to access grouped values be column only
                    if (q.getParts().size() == 2)
                    {
                        return Stream.of(q, QualifiedName.of(q.getLast()));
                    }

                    return Stream.of(q);
                })
                .collect(toSet());

        return new GroupByOperator(
                nodeId,
                operator,
                columnReferences,
                new ExpressionValuesExtractor(groupBys),
                groupBys.size());
    }
}
