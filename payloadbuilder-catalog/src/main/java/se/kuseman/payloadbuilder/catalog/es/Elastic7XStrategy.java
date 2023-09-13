package se.kuseman.payloadbuilder.catalog.es;

/** Strategy used for elastic 7.x versions */
class Elastic7XStrategy extends GenericStrategy
{
    @Override
    public boolean supportsTypes()
    {
        return false;
    }
}
