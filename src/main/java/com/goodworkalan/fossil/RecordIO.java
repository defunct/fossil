package com.goodworkalan.fossil;

import java.nio.ByteBuffer;

public interface RecordIO<T>
{
    public T read(ByteBuffer bytes);
    
    public void write(ByteBuffer bytes, T object);
    
    public int getSize();
}
