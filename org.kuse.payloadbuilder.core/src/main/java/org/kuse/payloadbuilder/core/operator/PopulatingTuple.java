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

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.kuse.payloadbuilder.core.parser.QualifiedName;

/**
 * Tuple that is composed of tuples of the same table source in a collection. type fashion.
 **/
class PopulatingTuple extends ArrayList<Tuple> implements Tuple
{
    /** Unique id of the operator node that produced this tuple */
    private final int nodeId;

    PopulatingTuple(int nodeId, Tuple tuple)
    {
        this.nodeId = nodeId;
        add(requireNonNull(tuple));
    }

    int getNodeId()
    {
        return nodeId;
    }

    List<Tuple> getTuples()
    {
        return this;
    }

    @Override
    public boolean containsAlias(String alias)
    {
        return get(0).containsAlias(alias);
    }

    @Override
    public Object getValue(QualifiedName qname, int partIndex)
    {
        // Getting a value from a populating tuple means
        // delegating to the first tuple simply
        return get(0).getValue(qname, partIndex);
    }

    @Override
    public Iterator<QualifiedName> getQualifiedNames()
    {
        return get(0).getQualifiedNames();
    }
}
