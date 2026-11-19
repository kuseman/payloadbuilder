package se.kuseman.payloadbuilder.catalog.es;

/** Strategy used for elastic 8.x versions */
class Elastic8XStrategy extends GenericStrategy
{
    @Override
    public boolean supportsTypes()
    {
        return false;
    }
}
