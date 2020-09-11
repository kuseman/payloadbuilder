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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/** Context used during selection of operator tree */
public class OperatorContext
{
    /** Stores node unique data by node's unique id */
    private final Map<Integer, NodeData> nodeDataById = new ConcurrentHashMap<>();

    /** Iterator of outer row values used when having an indexed inner operator in Batched operators */
    private Iterator<Object[]> outerIndexValues;

    public Iterator<Object[]> getOuterIndexValues()
    {
        return outerIndexValues;
    }

    public void setOuterIndexValues(Iterator<Object[]> outerIndexValues)
    {
        this.outerIndexValues = outerIndexValues;
    }

    public void clear()
    {
        outerIndexValues = null;
        nodeDataById.clear();
    }

    public Map<Integer, ? extends NodeData> getNodeData()
    {
        return nodeDataById;
    }

    /** Get node data by id */
    @SuppressWarnings("unchecked")
    public <T extends NodeData> T getNodeData(int nodeId)
    {
        return (T) nodeDataById.get(nodeId);
    }

    /** Get or create node data provided id */
    @SuppressWarnings("unchecked")
    public <T extends NodeData> T getNodeData(int nodeId, Supplier<T> creator)
    {
        return (T) nodeDataById.compute(nodeId, (k, v) ->
        {
            if (v == null)
            {
                v = creator.get();
            }
            v.executionCount++;
            return v;
        });
    }

    /** Base class for node data. */
    public static class NodeData
    {
        public int executionCount;
        /** Operator specific properties. Bytes fetched etc. */
        public Map<String, Object> properties = new HashMap<>();
    }
}
