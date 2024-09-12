package se.kuseman.payloadbuilder.catalog.es;

import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;

/** Strategy used when building queryies etc. Different strategies are used for different elastic versions */
interface ElasticStrategy
{
    /** Return scroll request from provided parts */
    HttpUriRequestBase getScrollRequest(String endpoint, String scrollId);

    /** Return delete scroll request from provided parts */
    HttpUriRequestBase getDeleteScrollRequest(String endpoint, String scrollId);

    /** Returns whether this elastic supports the filter clause in boolean queries or not */
    boolean supportsFilterInBoolQuery();

    /** Returns whether this elastic supports types or not */
    boolean supportsTypes();

    /** Returns whether this elastic needs the nested sort path in a nested object or a plain property */
    boolean wrapNestedSortPathInObject();

    /** Returns true if this elastic supports data streams or not. */
    boolean supportsDataStreams();
}
