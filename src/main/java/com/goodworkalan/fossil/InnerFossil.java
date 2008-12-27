package com.goodworkalan.fossil;

import java.nio.ByteBuffer;

import com.goodworkalan.pack.Mutator;
import com.goodworkalan.strata.Branch;
import com.goodworkalan.strata.ChildType;
import com.goodworkalan.strata.Cooper;
import com.goodworkalan.strata.Extractor;
import com.goodworkalan.strata.InnerStore;
import com.goodworkalan.strata.InnerTier;

public final class InnerFossil<T>
implements InnerStore<T, Long, Mutator>
{    
    private final RecordIO<T> recordIO;
    
    public InnerFossil(RecordIO<T> recordIO)
    {
        this.recordIO = recordIO;
    }
    
    private int getSize(int size)
    {
        return Fossil.SIZEOF_INTEGER + Fossil.SIZEOF_SHORT + (size * (recordIO.getSize() + Fossil.SIZEOF_LONG));
    }

    public Long allocate(Mutator mutator, int size)
    {
        return mutator.allocate(getSize(size));
    }
    
    public <B> InnerTier<B, Long> load(Mutator mutator, Long address, Cooper<T, B, Mutator> cooper, Extractor<T, Mutator> extractor)
    {
        InnerTier<B, Long> inner = new InnerTier<B, Long>();
        ByteBuffer bytes = mutator.read(address);
        int size = bytes.getInt();
        short type = bytes.getShort();
        inner.setChildType(type == 1 ? ChildType.INNER : ChildType.LEAF);
        for (int i = 0; i < size; i++)
        {
            T object = recordIO.read(bytes);
            Long childAddress = bytes.getLong();
            B bucket = cooper.newBucket(mutator, extractor, object);
            inner.add(new Branch<B, Long>(bucket, childAddress));
        }
        return inner;
    }
    
    public <B> void write(Mutator mutator, InnerTier<B, Long> inner, Cooper<T, B, Mutator> cooper, Extractor<T, Mutator> extractor)
    {
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
    
    public void free(Mutator mutator, Long address)
    {
        mutator.free(address);
    }
}
