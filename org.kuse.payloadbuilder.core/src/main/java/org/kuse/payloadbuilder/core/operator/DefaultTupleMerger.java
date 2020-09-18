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

/** Merges outer and inner tuple */
class DefaultTupleMerger implements TupleMerger
{
    /** Default merger. Merges inner row into outer */
    public static final DefaultTupleMerger DEFAULT = new DefaultTupleMerger(-1);

    /** Limit number of merged rows */
    private final int limit;

    DefaultTupleMerger(int limit)
    {
        this.limit = limit;
    }

    @Override
    public Tuple merge(Tuple outer, Tuple inner, boolean populating, int nodeId)
    {
        //        Tuple result = outer;

        // No populating merge, create/or merge a composite tuple
        if (!populating)
        {
            if (outer instanceof CompositeTuple)
            {
                CompositeTuple tuple = (CompositeTuple) outer;
                return new CompositeTuple(tuple, inner);
            }
            else if (inner instanceof CompositeTuple)
            {
                CompositeTuple tuple = (CompositeTuple) inner;
                return new CompositeTuple(outer, tuple);
            }

            return new CompositeTuple(outer, inner);
        }

        /*
         * a    b
         * 1    1,1
         * 2    1,2
         * 3    2,1
         *      2,2
         *
         * (s, [a], [an])     (aa, ap, a1) = > (s, [a], [an], [aa])
         *
         *
         */

        CompositeTuple outerTuple;
        PopulatingTuple populatingTuple;
        if (!(outer instanceof CompositeTuple))
        {
            populatingTuple = new PopulatingTuple(nodeId, inner);
            return new CompositeTuple(outer, populatingTuple);
        }

        outerTuple = (CompositeTuple) outer;
        // Fetch last tuple in the composite and see if that one belongs to the current merging node
        Tuple tuple = outerTuple.getTuples().get(outerTuple.getTuples().size() - 1);
        if (tuple instanceof PopulatingTuple && ((PopulatingTuple) tuple).getNodeId() == nodeId)
        {
            populatingTuple = (PopulatingTuple) tuple;
            populatingTuple.getTuples().add(inner);
        }
        else
        {
            populatingTuple = new PopulatingTuple(nodeId, inner);
            outerTuple.getTuples().add(populatingTuple);
        }

        return outerTuple;

        // Parent is always populated
        //        inner.addParent(result);
        //        List<Row> childRows = result.getChildRows(inner.getTableAlias());
        //        if (limit < 0 || childRows.size() < limit)
        //        {
        //            childRows.add(inner);
        //        }
        //        return result;
    }

    @Override
    public int hashCode()
    {
        return limit;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof DefaultTupleMerger)
        {
            return limit == ((DefaultTupleMerger) obj).limit;
        }
        return false;
    }

    /** Create a limiting row merger */
    static DefaultTupleMerger limit(int limit)
    {
        return new DefaultTupleMerger(limit);
    }
}
