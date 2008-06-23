/* Copyright Alan Gutierrez 2006 */
package com.agtrz.fossil.redux.Fossil;

import java.nio.ByteBuffer;
import java.util.Collection;

import com.agtrz.pack.Pack;
import com.agtrz.pack.Pack.Mutator;

public class Fossil
{
    public final static class Store<H, T>
    implements com.agtrz.strata.Store<Long, H, T, Pack.Mutator>
    {
        private static final long serialVersionUID = 1L;

        private HeaderIO<H> headerIO;
        
        private RecordIO<T> recordIO;
        
        private int size;
        
        public Long allocate(Mutator txn)
        {
            return txn.allocate(0);
        }
        
        public H load(Mutator txn, Long address, Collection<T> collection)
        {
            ByteBuffer bytes = txn.read(address);
            int size = bytes.getInt();
            H header = headerIO.read(bytes);
            for (int i = 0; i < size; i++)
            {
                collection.add(recordIO.read(bytes));
            }
            return header;
        }
        
        public void write(Mutator txn, Long address, H header, Collection<T> collection)
        {
            ByteBuffer bytes = ByteBuffer.allocateDirect(size);
            
            bytes.putInt(collection.size());
            headerIO.write(bytes, header);
            for (T object : collection)
            {
                recordIO.write(bytes, object);
            }
            
            bytes.flip();
            
            txn.write(address, bytes);
        }
        
        public void free(Mutator txn, Long address)
        {
            txn.free(address);
        }
    }
    
    public interface RecordIO<T>
    {
        public T read(ByteBuffer bytes);
        
        public void write(ByteBuffer bytes, T object);
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */