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

import static java.util.Collections.emptyIterator;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.kuse.payloadbuilder.core.parser.ExecutionContext;

/** Definition of a selection operator */
public interface Operator
{
    /** Open iterator */
    RowIterator open(ExecutionContext context);

    /** Get node id of operator */
    int getNodeId();

    /** Return child operators if any */
    default List<Operator> getChildOperators()
    {
        return emptyList();
    }

    /** Returns name of operator when used in describe/analyze statements etc. */
    default String getName()
    {
        return getClass().getSimpleName();
    }

    /**
     * Returns a short describe string of the operator. Used in describe statement
     */
    default String getDescribeString()
    {
        return "";
    }

    /** Returns more detail properties of describe statement if {@link #getDescribeString()} is not enough. */
    default Map<String, Object> getDescribeProperties()
    {
        return emptyMap();
    }

    /**
     * To string with indent. Used when printing operator tree
     *
     * @param indent Indent count
     */
    default String toString(int indent)
    {
        return toString();
    }

    /** Definition of a iterator that stream {@link Tuple}'s */
    public interface RowIterator extends Iterator<Tuple>
    {
        public static RowIterator EMPTY = wrap(emptyIterator());

        public static RowIterator wrap(Iterator<Tuple> iterator)
        {
            return new RowIterator()
            {
                @Override
                public Tuple next()
                {
                    return iterator.next();
                }

                @Override
                public boolean hasNext()
                {
                    return iterator.hasNext();
                }
            };
        }

        /** Close iterator */
        default void close()
        {
        }
    }
}
