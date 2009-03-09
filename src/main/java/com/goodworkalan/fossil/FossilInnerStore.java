package com.goodworkalan.fossil;

import java.nio.ByteBuffer;
import java.util.Collection;

import com.goodworkalan.pack.Mutator;
import com.goodworkalan.stash.Stash;
import com.goodworkalan.strata.Branch;
import com.goodworkalan.strata.ChildType;
import com.goodworkalan.strata.InnerStore;

/**
 * An implementation of an inner tier store that writes branches to a block in a
 * pack file.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the b+tree objects.
 */
class FossilInnerStore<T>
extends FossilStore<T>
implements InnerStore<T, Long>
{
    /**
     * Create a persistent storage strategy for inner tier branches with
     * the given record I/O strategy.
     * 
     * @param recordIO
     *            The strategy to read and write an individual value object.
     */
    public FossilInnerStore(RecordIO<T> recordIO)
    {     
       super(recordIO);
    }

    /**
     * Get the size of an inner tier in bytes.
     * 
     * @param capacity
     *            The count of branches.
     * @return The size of the inner tier in bytes.
     */
    protected int getByteSize(int capacity)
    {
        return Fossil.SIZEOF_INTEGER + Fossil.SIZEOF_SHORT + (capacity * (recordIO.getSize() + Fossil.SIZEOF_LONG));
    }

    /**
     * Load a collection of inner tier branches from the persistent storage at
     * the given address.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param address
     *            The address of the inner tier storage.
     * @param objects
     *            The collection to load.
     * @return The child type of the loaded collection or null if the collection
     *         is a leaf.
     */
    public ChildType load(Stash stash, Long address, Collection<Branch<T, Long>> objects)
    {  
        Mutator mutator = stash.get(Fossil.MUTATOR, Mutator.class);

        ByteBuffer bytes = mutator.read(address);
        int size = bytes.getInt();
        short type = bytes.getShort();
        for (int i = 0; i < size; i++)
        {
            T object = recordIO.read(bytes);
            Long childAddress = bytes.getLong();
            objects.add(new Branch<T, Long>(object, childAddress));
        }
        return type == 1 ? ChildType.INNER : ChildType.LEAF;
    }

    /**
     * Write a collection of inner tier branches to the persistent storage at
     * the given address.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param address
     *            The address of the inner tier storage.
     * @param branches
     *            The branches to write.
     * @param childType
     *            The child type.
     */
    public void write(Stash stash, Long address, Collection<Branch<T, Long>> branches, ChildType childType)
    {
        Mutator mutator = stash.get(Fossil.MUTATOR, Mutator.class);

        ByteBuffer byteBuffer = ByteBuffer.allocate(getByteSize(branches.size()));
        byteBuffer.putInt(branches.size());
        byteBuffer.putShort(childType == ChildType.INNER ? (short) 1 : (short) 2);
        for (Branch<T, Long> branch: branches) 
        {
            recordIO.write(byteBuffer, branch.getPivot());
            byteBuffer.putLong(branch.getAddress());
        }
        byteBuffer.flip();
        
        mutator.write(address, byteBuffer);
    }
}
