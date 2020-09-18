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

import java.util.Iterator;

import org.kuse.payloadbuilder.core.parser.QualifiedName;

/** Tuple used during join that connects two tuples and checks match */
class JoinTuple implements Tuple
{
    /** Tuple used when having a correlated nested loop, and points to the current outer tuple */
    private Tuple contextOuter;
    private Tuple outer;
    private Tuple inner;

    public Tuple getContextOuter()
    {
        return contextOuter;
    }

    public void setContextOuter(Tuple contextOuter)
    {
        this.contextOuter = contextOuter;
    }

    public Tuple getOuter()
    {
        return outer;
    }

    public void setOuter(Tuple outer)
    {
        this.outer = outer;
    }

    public Tuple getInner()
    {
        return inner;
    }

    public void setInner(Tuple inner)
    {
        this.inner = inner;
    }

    @Override
    public boolean containsAlias(String alias)
    {
        return inner.containsAlias(alias)
            || (outer == null || outer.containsAlias(alias))
            || (contextOuter == null || contextOuter.containsAlias(alias));
    }

    @Override
    public Object getValue(QualifiedName qname, int partIndex)
    {
        // Single part => inner
        if (qname.getParts().size() == 1)
        {
            return inner.getValue(qname, 0);
        }

        // This tuple is always called on root and index 0 can be assumed
        String part = qname.getParts().get(0);

        if (inner.containsAlias(part))
        {
            return inner.getValue(qname, 0);
        }
        else if (outer != null && outer.containsAlias(part))
        {
            return outer.getValue(qname, 0);
        }
        else if (contextOuter != null)
        {
            return contextOuter.getValue(qname, 1);
        }

        return null;
    }

    @Override
    public Iterator<QualifiedName> getQualifiedNames()
    {
        return emptyIterator();
    }
}
