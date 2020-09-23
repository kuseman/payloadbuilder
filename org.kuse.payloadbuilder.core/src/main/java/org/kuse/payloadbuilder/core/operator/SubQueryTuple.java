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

import java.util.Iterator;

import org.kuse.payloadbuilder.core.parser.QualifiedName;

/** Tuple that is a result from a sub query */
class SubQueryTuple implements Tuple
{
    private final Tuple tuple;
    private final String alias;

    SubQueryTuple(Tuple tuple, String alias)
    {
        this.tuple = tuple;
        this.alias = requireNonNull(alias, "alias");
    }

    @Override
    public boolean containsAlias(String alias)
    {
        return this.alias.equalsIgnoreCase(alias);
    }

    @Override
    public Object getValue(QualifiedName qname, int partIndex)
    {
        int index = partIndex;
        if (qname.getParts().size() > partIndex && containsAlias(qname.getParts().get(partIndex)))
        {
            index++;
        }
        
        return tuple.getValue(qname, index);
    }

    @Override
    public Iterator<QualifiedName> getQualifiedNames()
    {
        return tuple.getQualifiedNames();
    }
}
