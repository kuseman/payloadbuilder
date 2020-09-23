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

import static java.util.Spliterators.spliteratorUnknownSize;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.StreamSupport;

import org.kuse.payloadbuilder.core.parser.QualifiedName;

/** Tuple that is composed of other tuples */
class CompositeTuple extends ArrayList<Tuple> implements Tuple
{
    CompositeTuple(Tuple outer, Tuple inner)
    {
        add(outer);
        add(inner);
    }

    CompositeTuple(CompositeTuple outer, Tuple inner)
    {
        addAll(outer);
        add(inner);
    }

    CompositeTuple(Tuple outer, CompositeTuple inner)
    {
        add(outer);
        addAll(inner);
    }

    List<Tuple> getTuples()
    {
        return this;
    }

    @Override
    public boolean containsAlias(String alias)
    {
        int size = size();
        for (int i = 0; i < size; i++)
        {
            Tuple tuple = get(i);
            if (tuple.containsAlias(alias))
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public Object getValue(QualifiedName qname, int partIndex)
    {
        if (partIndex >= qname.getParts().size())
        {
            System.err.println();
        }
        String part = qname.getParts().get(partIndex);

        int size = size();
        for (int i = 0; i < size; i++)
        {
            Tuple tuple = get(i);
            if (tuple.containsAlias(part))
            {
                // This is the last part in the qualified name => return the tuple itself
                if (partIndex == qname.getParts().size() - 1)
                {
                    return tuple;
                }
                return tuple.getValue(qname, partIndex + 1);
            }
        }

        return get(0).getValue(qname, partIndex);
    }

    @Override
    public Iterator<QualifiedName> getQualifiedNames()
    {
        return stream()
                .flatMap(tuple -> StreamSupport.stream(spliteratorUnknownSize(tuple.getQualifiedNames(), 0), false))
                .iterator();
    }

    //    @Override
    //    public void writeColumns(OutputWriter writer, String alias)
    //    {
    //        int size = size();
    //        for (int i = 0; i < size; i++)
    //        {
    //            Tuple tuple = get(i);
    //            if (alias == null || tuple.containsAlias(alias))
    //            {
    //                tuple.writeColumns(writer, alias);
    //            }
    //        }
    //    }
}
