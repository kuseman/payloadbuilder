package se.kuseman.payloadbuilder.bytes.example;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.SerializableString;
import com.fasterxml.jackson.core.io.SerializedString;

import se.kuseman.payloadbuilder.api.execution.UTF8String;

/**
 * Writes an {@link IProduct} as JSON without ever materializing a {@link String} for its text fields - this is the low-allocation flow the {@link UTF8String} return type on
 * {@link IProduct#getName()}/{@link ISku#getName()} is for.
 *
 * <p>
 * {@link UTF8String#getBytes(byte[])} on a payload-backed value is a raw {@code arraycopy} out of the payload's own backing byte array - no UTF-8 decode happens, unlike {@code toString()} which
 * allocates both a {@code char[]} and a {@link String}. Copying into a buffer reused across calls (here, per-thread) means that after the buffer has grown to the largest name ever seen, writing every
 * subsequent product's name costs zero allocation. {@link JsonGenerator#writeUTF8String(byte[], int, int)} then writes those raw bytes straight to the output stream, doing its escaping analysis on
 * bytes directly.
 * </p>
 *
 * <p>
 * Two things this depends on: the {@link JsonGenerator} must be backed by an {@code OutputStream} (byte oriented), not a {@code Writer} (char oriented) - only the former gets Jackson's optimized byte
 * path. And the reused buffer is only a win if the same underlying {@link Thread} instance actually serves many requests over time - see the platform-thread-vs-virtual-thread discussion this pattern
 * came out of; a fresh {@link Thread} per request (eg. one virtual thread per request) gets no reuse benefit from a {@link ThreadLocal} here.
 * </p>
 */
final class ProductJsonWriter
{
    // Grows to the largest name ever seen on this thread and never shrinks - after warmup, writing a name is a
    // single arraycopy plus Jackson's own byte-level write, no allocation at all.
    private static final ThreadLocal<byte[]> UTF8_BUFFER = ThreadLocal.withInitial(() -> new byte[64]);

    private static final SerializableString ID = new SerializedString("id");
    private static final SerializableString NAME = new SerializedString("name");
    private static final SerializableString PRICE = new SerializedString("price");

    private ProductJsonWriter()
    {
    }

    static void write(JsonGenerator generator, IProduct product) throws IOException
    {
        generator.writeStartObject();
        generator.writeFieldName(ID);
        generator.writeNumber(product.getId());
        writeUtf8Field(generator, NAME, product.getName());
        generator.writeFieldName(PRICE);
        generator.writeNumber(product.getPrice()
                .asBigDecimal());

        generator.writeArrayFieldStart("skus");
        for (ISku sku : product.getSkus())
        {
            generator.writeStartObject();
            generator.writeFieldName(ID);
            generator.writeNumber(sku.getId());
            writeUtf8Field(generator, NAME, sku.getName());
            generator.writeFieldName(PRICE);
            generator.writeNumber(sku.getPrice()
                    .asBigDecimal());
            generator.writeEndObject();
        }
        generator.writeEndArray();

        generator.writeEndObject();
    }

    private static void writeUtf8Field(JsonGenerator generator, SerializableString fieldName, UTF8String value) throws IOException
    {
        generator.writeFieldName(fieldName);
        if (value == null)
        {
            generator.writeNull();
            return;
        }
        writeUtf8String(generator, value);
    }

    private static void writeUtf8String(JsonGenerator generator, UTF8String value) throws IOException
    {
        int length = value.getByteLength();
        byte[] buffer = UTF8_BUFFER.get();
        if (buffer.length < length)
        {
            buffer = new byte[length];
            UTF8_BUFFER.set(buffer);
        }
        value.getBytes(buffer);
        generator.writeUTF8String(buffer, 0, length);
    }
}
