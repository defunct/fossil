package com.goodworkalan.fossil;

import java.nio.ByteBuffer;

/**
 * A strategy to read and write an individual value object.
 * 
 * @author Alan Gutierrez
 *
 * @param <T>
 *            The value type of the b+tree objects.
 */
public interface RecordIO<T>
{
    /**
     * Read a single object from the given byte buffer.
     * 
     * @param byteBuffer
     *            The byte buffer.
     * @return A single object read form the byte buffer.
     */
    public T read(ByteBuffer byteBuffer);

    /**
     * Write the given object to the given byte buffer.
     * 
     * @param byteBuffer
     *            The byte buffer.
     * @param object
     *            The object to write.
     */
    public void write(ByteBuffer byteBuffer, T object);
    
    /**
     * Get the size in bytes of a single record.
     * 
     * @return The size in bytes of a single record.
     */
    public int getSize();
}
