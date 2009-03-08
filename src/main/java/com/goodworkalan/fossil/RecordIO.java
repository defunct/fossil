package com.goodworkalan.fossil;

import java.nio.ByteBuffer;

// TODO Document.
public interface RecordIO<T>
{
    // TODO Document.
    public T read(ByteBuffer byteBuffer);
    
    // TODO Document.
    public void write(ByteBuffer byteBuffer, T object);
    
    // TODO Document.
    public int getSize();
}
