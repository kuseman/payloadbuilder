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
import java.util.List;

import org.kuse.payloadbuilder.core.OutputWriter;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;

/** Array projection. Projects a list of sub projections over a selection */
class ArrayProjection implements Projection
{
    private final List<Projection> projections;
    private final Operator selection;

    ArrayProjection(List<Projection> projections, Operator selection)
    {
        this.projections = requireNonNull(projections);
        this.selection = selection;
    }

    @Override
    public void writeValue(OutputWriter writer, ExecutionContext context)
    {
        int size = projections.size();

        writer.startArray();
        if (selection != null)
        {
            Tuple prevTuple = context.getTuple();
            Iterator<Tuple> it = selection.open(context);
            while (it.hasNext())
            {
                Tuple tuple = it.next();
                for (int i = 0; i < size; i++)
                {
                    context.setTuple(tuple);
                    projections.get(i).writeValue(writer, context);
                }
            }
            context.setTuple(prevTuple);
        }
        else
        {
            for (int i = 0; i < size; i++)
            {
                projections.get(i).writeValue(writer, context);
            }
        }
        writer.endArray();
    }

    @Override
    public int hashCode()
    {
        return 17 +
            37 * selection.hashCode() +
            37 * projections.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof ArrayProjection)
        {
            ArrayProjection p = (ArrayProjection) obj;
            return selection.equals(p.selection)
                && projections.equals(p.projections);
        }
        return false;
    }
}
