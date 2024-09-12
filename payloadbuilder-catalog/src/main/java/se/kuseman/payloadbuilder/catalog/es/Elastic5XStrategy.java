package se.kuseman.payloadbuilder.catalog.es;

/** Strategy used for elastic 5.x versions */
class Elastic5XStrategy extends GenericStrategy
{
    @Override
    public boolean wrapNestedSortPathInObject()
    {
        return false;
    }

    @Override
    public boolean supportsDataStreams()
    {
        return false;
    }
}
