package se.kuseman.payloadbuilder.catalog.es;

/** Strategy used for elastic 6.x versions */
class Elastic6XStrategy extends GenericStrategy
{
    @Override
    public boolean supportsTypes()
    {
        return false;
    }

    @Override
    public boolean supportsDataStreams()
    {
        return false;
    }
}
