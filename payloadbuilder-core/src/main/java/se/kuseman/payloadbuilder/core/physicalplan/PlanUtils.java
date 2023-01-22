package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Collections.singletonList;

import java.util.ArrayList;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.TupleIterator;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.utils.VectorUtils;

/** Utils for plans */
class PlanUtils
{
    /** Concats provided iterator into a single tuple vector. NOTE! Closes the iterator */
    static TupleVector concat(TupleIterator iterator)
    {
        List<TupleVector> vectors = null;
        try
        {
            while (iterator.hasNext())
            {
                TupleVector vector = iterator.next();

                if (vectors == null)
                {
                    vectors = singletonList(vector);
                }
                else
                {
                    // Switch to array list
                    if (vectors.size() == 1)
                    {
                        List<TupleVector> tmp = new ArrayList<>(5);
                        tmp.addAll(vectors);
                        vectors = tmp;
                    }
                    vectors.add(vector);
                }
            }
        }
        finally
        {
            iterator.close();
        }

        if (vectors == null)
        {
            return TupleVector.EMPTY;
        }

        return VectorUtils.merge(vectors);
    }
}
