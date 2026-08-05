package se.kuseman.payloadbuilder.catalog.mongodb;

import java.util.ArrayList;
import java.util.List;

import org.bson.conversions.Bson;

import com.mongodb.client.model.Sorts;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.ISortItem;

/** Translates PLB sort items into a MongoDB {@code $sort} document. Sort pushdown is all-or-none. */
final class MongoSortBuilder
{
    private MongoSortBuilder()
    {
    }

    /** Returns the sort document if all provided sort items could be pushed down, consuming them from the (mutable) list. Returns null otherwise, leaving the list untouched. */
    static Bson build(List<? extends ISortItem> sortItems)
    {
        if (sortItems.isEmpty())
        {
            return null;
        }

        List<Bson> orders = new ArrayList<>(sortItems.size());
        for (ISortItem item : sortItems)
        {
            // Mongo's $sort has no null-ordering control
            if (item.getNullOrder() != ISortItem.NullOrder.UNDEFINED)
            {
                return null;
            }

            QualifiedName qname = item.getExpression()
                    .getQualifiedColumn();
            if (qname == null)
            {
                return null;
            }

            String field = qname.toDotDelimited();
            orders.add(item.getOrder() == ISortItem.Order.ASC ? Sorts.ascending(field)
                    : Sorts.descending(field));
        }

        sortItems.clear();
        return Sorts.orderBy(orders);
    }
}
