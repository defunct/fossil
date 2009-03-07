package com.goodworkalan.fossil;

import java.nio.ByteBuffer;

// TODO Document.
public interface RecordIO<T>
{
    // TODO Document.
    public T read(ByteBuffer bytes);
    
    // TODO Document.
    public void write(ByteBuffer bytes, T object);
    
    // TODO Document.
    public int getSize();
}
