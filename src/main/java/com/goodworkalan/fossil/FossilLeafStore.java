package com.goodworkalan.fossil;

import java.nio.ByteBuffer;

import com.goodworkalan.favorites.Stash;
import com.goodworkalan.pack.Mutator;
import com.goodworkalan.strata.Cooper;
import com.goodworkalan.strata.Extractor;
import com.goodworkalan.strata.LeafStore;
import com.goodworkalan.strata.LeafTier;

public class FossilLeafStore<T, F extends Comparable<F>>
implements LeafStore<T, F, Long>
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
    
    public Long allocate(Stash stash, int size)
    {
        Mutator mutator = stash.get(Fossil.MUTATOR, Mutator.class);
        return mutator.allocate(size);
    }

    public <B> LeafTier<B, Long> load(Stash stash, Long address, Cooper<T, F, B> cooper, Extractor<T, F> extractor)
    {
        LeafTier<B, Long> leaf = new LeafTier<B, Long>();

        Mutator mutator = stash.get(Fossil.MUTATOR, Mutator.class);
        
        ByteBuffer bytes = mutator.read(address);
        int size = bytes.getInt();

        leaf.setNext(bytes.getLong());
        
        for (int i = 0; i < size; i++)
        {
            T object = recordIO.read(bytes);
            B bucket = cooper.newBucket(stash, extractor, object);
            leaf.add(bucket);
        }
        
        return leaf;
    }
    
    public <B> void write(Stash stash, LeafTier<B, Long> leaf, Cooper<T, F, B> cooper, Extractor<T, F> extractor)
    {
        Mutator mutator = stash.get(Fossil.MUTATOR, Mutator.class);

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
    
    public void free(Stash stash, Long address)
    {
        Mutator mutator = stash.get(Fossil.MUTATOR, Mutator.class);
        mutator.free(address);
    }
}


/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */