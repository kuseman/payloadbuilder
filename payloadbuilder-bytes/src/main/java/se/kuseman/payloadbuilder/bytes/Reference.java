package se.kuseman.payloadbuilder.bytes;

/** A mutable reference */
class Reference<T>
{
    private T value;

    void set(T value)
    {
        this.value = value;
    }

    public T getValue()
    {
        return value;
    }
}
