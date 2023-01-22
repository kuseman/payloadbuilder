package se.kuseman.payloadbuilder.core.physicalplan;

import java.util.ArrayList;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.utils.VectorUtils;

/** Utils for creating joined {@link TupleVector}'s etc. */
final class JoinUtils
{
    private JoinUtils()
    {
    }

    /** Cross joins two tuple vectors with provided predicate. */
    static TupleVector crossJoin(TupleVector outer, TupleVector inner, String populateAlias)
    {
        if (populateAlias == null)
        {
            // Return the cartesian of outer/inner since that is our result with a non populating cross join
            return VectorUtils.cartesian(outer, inner);
        }

        // Combine the outer vectors with a replicated populated inner vector
        int outerSize = outer.getSchema()
                .getSize();
        List<ValueVector> vectors = new ArrayList<>(outerSize + 1);
        // First add the outer vectors
        for (int i = 0; i < outerSize; i++)
        {
            vectors.add(outer.getColumn(i));
        }
        // Then add a a populating ValueVector with the size of outer
        vectors.add(ValueVector.literalObject(ResolvedType.tupleVector(inner.getSchema()), inner, outer.getRowCount()));

        Schema schema = outer.getSchema()
                .populate(populateAlias, inner.getSchema());
        return TupleVector.of(schema, vectors);
    }
}
