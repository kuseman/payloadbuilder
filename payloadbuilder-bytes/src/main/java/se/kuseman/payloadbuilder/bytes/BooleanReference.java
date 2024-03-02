package se.kuseman.payloadbuilder.bytes;

/** A mutable boolean reference */
class BooleanReference
{
    private boolean value;

    BooleanReference(boolean value)
    {
        this.value = value;
    }

    void set(boolean value)
    {
        this.value = value;
    }

    public boolean getValue()
    {
        return value;
    }
}
