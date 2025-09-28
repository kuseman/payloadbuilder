package se.kuseman.payloadbuilder.api.catalog;

import static java.util.Collections.emptyMap;

import java.util.Map;

import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;

/** Definition of a data source. The plugable extension point for scans for catalogs */
public interface IDatasource
{
    // Common constants used in getDescribeProperties
    public static final String CATALOG = "Catalog";
    public static final String PREDICATE = "Predicate";
    public static final String OUTPUT = "Output";
    public static final String DEFINED_VALUES = "Defined Values";
    public static final String INDEX = "Index";

    /** Execute data source returning a stream of tuple vectors */
    TupleIterator execute(IExecutionContext context);

    /**
     * Returns a map with describe properties that is used during describe/analyze statements
     */
    default Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        return emptyMap();
    }
}
