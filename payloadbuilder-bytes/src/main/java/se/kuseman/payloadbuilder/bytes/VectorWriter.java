package se.kuseman.payloadbuilder.bytes;

import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Definition of a vector writer */
interface VectorWriter
{
    /** Return version used by this writer */
    byte getVersion();

    /** Write provided vector */
    void write(BytesWriter writer, WriteCache cache, ValueVector vector, int from, int to, int nullCount);
}
