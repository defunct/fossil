package com.goodworkalan.fossil;

import java.nio.ByteBuffer;

import com.goodworkalan.pack.Mutator;
import com.goodworkalan.strata.Cooper;
import com.goodworkalan.strata.Extractor;
import com.goodworkalan.strata.LeafStore;
import com.goodworkalan.strata.LeafTier;

public class FossilLeafStore<T>
implements LeafStore<T, Long, Mutator>
{
    private final RecordIO<T> recordIO;
    
    public FossilLeafStore(RecordIO<T> recordIO)
    {
        this.recordIO = recordIO;
    }
    
    private int getSize(int size)
    {
        return Fossil.SIZEOF_INTEGER + Fossil.SIZEOF_LONG + (size * recordIO.getSize());
    }
    
    public Long allocate(Mutator mutator, int size)
    {
        return mutator.allocate(size);
    }

    public <B> LeafTier<B, Long> load(Mutator mutator, Long address, Cooper<T, B, Mutator> cooper, Extractor<T, Mutator> extractor)
    {
        LeafTier<B, Long> leaf = new LeafTier<B, Long>();

        ByteBuffer bytes = mutator.read(address);
        int size = bytes.getInt();

        leaf.setNext(bytes.getLong());
        
        for (int i = 0; i < size; i++)
        {
            T object = recordIO.read(bytes);
            B bucket = cooper.newBucket(mutator, extractor, object);
            leaf.add(bucket);
        }
        
        return leaf;
    }
    
    public <B> void write(Mutator mutator, LeafTier<B, Long> leaf, Cooper<T, B, Mutator> cooper, Extractor<T, Mutator> extractor)
    {
        ByteBuffer bytes = ByteBuffer.allocate(getSize(leaf.size()));
        
        bytes.putInt(leaf.size());
        bytes.putLong(leaf.getNext());
        for (int i = 0; i < leaf.size(); i++)
        {
            recordIO.write(bytes, cooper.getObject(leaf.get(i)));
        }
        bytes.flip();
        mutator.write(leaf.getAddress(), bytes);
    }
    
    public void free(Mutator mutator, Long address)
    {
        mutator.free(address);
    }
}


/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */