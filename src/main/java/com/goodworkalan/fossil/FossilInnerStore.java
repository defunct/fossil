package com.goodworkalan.fossil;

import java.nio.ByteBuffer;

import com.goodworkalan.stash.Stash;
import com.goodworkalan.pack.Mutator;
import com.goodworkalan.strata.Branch;
import com.goodworkalan.strata.ChildType;
import com.goodworkalan.strata.Cooper;
import com.goodworkalan.strata.Extractor;
import com.goodworkalan.strata.InnerStore;
import com.goodworkalan.strata.InnerTier;

public final class FossilInnerStore<T, F extends Comparable<? super F>>
implements InnerStore<T, F, Long>
{    
    private final RecordIO<T> recordIO;
    
    public FossilInnerStore(RecordIO<T> recordIO)
    {
        this.recordIO = recordIO;
    }
    
    private int getSize(int size)
    {
        return Fossil.SIZEOF_INTEGER + Fossil.SIZEOF_SHORT + (size * (recordIO.getSize() + Fossil.SIZEOF_LONG));
    }

    public Long allocate(Stash stash, int size)
    {
        Mutator mutator = stash.get(Fossil.MUTATOR, Mutator.class);
        return mutator.allocate(getSize(size));
    }
    
    public <B> InnerTier<B, Long> load(Stash stash, Long address, Cooper<T, F, B> cooper, Extractor<T, F> extractor)
    {
        Mutator mutator = stash.get(Fossil.MUTATOR, Mutator.class);

        InnerTier<B, Long> inner = new InnerTier<B, Long>();
        ByteBuffer bytes = mutator.read(address);
        int size = bytes.getInt();
        short type = bytes.getShort();
        inner.setChildType(type == 1 ? ChildType.INNER : ChildType.LEAF);
        for (int i = 0; i < size; i++)
        {
            T object = recordIO.read(bytes);
            Long childAddress = bytes.getLong();
            B bucket = cooper.newBucket(stash, extractor, object);
            inner.add(new Branch<B, Long>(bucket, childAddress));
        }
        return inner;
    }
    
    public <B> void write(Stash stash, InnerTier<B, Long> inner, Cooper<T, F, B> cooper, Extractor<T, F> extractor)
    {
        Mutator mutator = stash.get(Fossil.MUTATOR, Mutator.class);

        ByteBuffer bytes = ByteBuffer.allocate(getSize(inner.size()));
        bytes.putInt(inner.size());
        bytes.putShort(inner.getChildType() == ChildType.INNER ? (short) 1 : (short) 2);
        for (int i = 0; i < inner.size(); i++)
        {
            T object = cooper.getObject(inner.get(i).getPivot());
            recordIO.write(bytes, object);
            bytes.putLong(inner.get(i).getAddress());
        }
        bytes.flip();
        mutator.write(inner.getAddress(), bytes);
    }
    
    public void free(Stash stash, Long address)
    {
        Mutator mutator = stash.get(Fossil.MUTATOR, Mutator.class);
        mutator.free(address);
    }
}
