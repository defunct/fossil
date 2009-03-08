package com.goodworkalan.fossil;

import java.nio.ByteBuffer;

import com.goodworkalan.pack.Mutator;
import com.goodworkalan.stash.Stash;
import com.goodworkalan.strata.Allocator;
import com.goodworkalan.strata.Branch;
import com.goodworkalan.strata.ChildType;
import com.goodworkalan.strata.InnerTier;
import com.goodworkalan.strata.LeafTier;

// TODO Document.
public class FossilAllocator<T> implements Allocator<T, Long>
{
    // TODO Document.
    private final RecordIO<T> recordIO;
    
    // TODO Document.
    public FossilAllocator(RecordIO<T> recordIO)
    {
        this.recordIO = recordIO;
    }
    
    // TODO Document.
    private int getInnerSize(int size)
    {
        return Fossil.SIZEOF_INTEGER + Fossil.SIZEOF_SHORT + (size * (recordIO.getSize() + Fossil.SIZEOF_LONG));
    }
 
    // TODO Document.
   public Long allocate(Stash stash, InnerTier<T, Long> inner, int capacity)
    {
        Mutator mutator = stash.get(Fossil.MUTATOR, Mutator.class);
        return mutator.allocate(getInnerSize(capacity));
    }
    
    // TODO Document.
    public void load(Stash stash, Long address, InnerTier<T, Long> inner)
    {  
        Mutator mutator = stash.get(Fossil.MUTATOR, Mutator.class);

        ByteBuffer bytes = mutator.read(address);
        int size = bytes.getInt();
        short type = bytes.getShort();
        inner.setChildType(type == 1 ? ChildType.INNER : ChildType.LEAF);
        for (int i = 0; i < size; i++)
        {
            T object = recordIO.read(bytes);
            Long childAddress = bytes.getLong();
            inner.add(new Branch<T, Long>(object, childAddress));
        }
    }
    
    // TODO Document.
    public void write(Stash stash, InnerTier<T, Long> inner)
    {
        Mutator mutator = stash.get(Fossil.MUTATOR, Mutator.class);

        ByteBuffer byteBuffer = ByteBuffer.allocate(getInnerSize(inner.size()));
        byteBuffer.putInt(inner.size());
        byteBuffer.putShort(inner.getChildType() == ChildType.INNER ? (short) 1 : (short) 2);
        for (int i = 0; i < inner.size(); i++)
        {
            T object = inner.get(i).getPivot();
            recordIO.write(byteBuffer, object);
            byteBuffer.putLong(inner.get(i).getAddress());
        }
        byteBuffer.flip();
        
        mutator.write(inner.getAddress(), byteBuffer);
    }
    
    // TODO Document.
    public void free(Stash stash, InnerTier<T, Long> inner)
    {
        Mutator mutator = stash.get(Fossil.MUTATOR, Mutator.class);
        mutator.free(inner.getAddress());
    }
    
    // TODO Document.
    private int getLeafSize(int size)
    {
        return Fossil.SIZEOF_INTEGER + Fossil.SIZEOF_LONG + (size * recordIO.getSize());
    }
    
    // TODO Document.
    public Long allocate(Stash stash, LeafTier<T, Long> leaf, int capacity)
    {
        Mutator mutator = stash.get(Fossil.MUTATOR, Mutator.class);
        return mutator.allocate(capacity);
    }
    
    // TODO Document.
    public void load(Stash stash, Long address, LeafTier<T, Long> leaf)
    {
        Mutator mutator = stash.get(Fossil.MUTATOR, Mutator.class);
        
        ByteBuffer bytes = mutator.read(address);
        int size = bytes.getInt();

        leaf.setNext(bytes.getLong());
        
        for (int i = 0; i < size; i++)
        {
            T object = recordIO.read(bytes);
            leaf.add(object);
        }
    }
    
    // TODO Document.
    public void write(Stash stash, LeafTier<T, Long> leaf)
    {
        Mutator mutator = stash.get(Fossil.MUTATOR, Mutator.class);

        ByteBuffer bytes = ByteBuffer.allocate(getLeafSize(leaf.size()));
        
        bytes.putInt(leaf.size());
        bytes.putLong(leaf.getNext());
        for (int i = 0; i < leaf.size(); i++)
        {
            recordIO.write(bytes, leaf.get(i));
        }
        bytes.flip();
        mutator.write(leaf.getAddress(), bytes);
    }
    
    // TODO Document.
    public void free(Stash stash, LeafTier<T, Long> leaf)
    {
        Mutator mutator = stash.get(Fossil.MUTATOR, Mutator.class);
        mutator.free(leaf.getAddress());
    }
    
    // TODO Document.
    public Long getNull()
    {
        return 0L;
    }
    
    // TODO Document.
    public boolean isNull(Long address)
    {
        return address == 0L;
    }
}
