package com.goodworkalan.fossil;

import java.nio.ByteBuffer;
import java.util.Collection;

import com.goodworkalan.pack.Mutator;
import com.goodworkalan.stash.Stash;
import com.goodworkalan.strata.LeafStore;

/**
 * An implementation of an leaf tier store that writes value objects to a block
 * in a pack file.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the b+tree objects.
 */
public class FossilLeafStore<T>
extends FossilStore<T>
implements LeafStore<T, Long>
{
    /**
     * Create a persistent storage strategy for leaf tier value objects with the
     * given record I/O strategy.
     * 
     * @param recordIO
     *            The strategy to read and write an individual value object.
     */
    public FossilLeafStore(RecordIO<T> recordIO)
    {
        super(recordIO);
    }

    /**
     * Get the size of an leaf tier in bytes.
     * 
     * @param capacity
     *            The count of value objects.
     * @return The size of the leaf tier in bytes.
     */
    protected int getByteSize(int capacity)
    {
        return Fossil.SIZEOF_INTEGER + Fossil.SIZEOF_LONG + (capacity * recordIO.getSize());
    }

    /**
     * Load a collection of value objects from the persistent storage at the
     * given address.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param address
     *            The address of the leaf tier storage.
     * @param objects
     *            The collection to load.
     * @return The address of the next leaf in the b-tree.
     */
    public Long load(Stash stash, Long address, Collection<T> objects)
    {
        Mutator mutator = stash.get(Fossil.MUTATOR, Mutator.class);
        
        ByteBuffer bytes = mutator.read(address);
        int size = bytes.getInt();

        long next = bytes.getLong();
        
        for (int i = 0; i < size; i++)
        {
            T object = recordIO.read(bytes);
            objects.add(object);
        }
        
        return next;
    }

    /**
     * Write a collection of leaf tier value objects to the persistent storage
     * at the given address.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param address
     *            The address of the leaf tier storage.
     * @param objects
     *            The collection to write.
     * @param extra
     *            Extra tier specific data.
     */
    public void write(Stash stash, Long address, Collection<T> objects, Long next)
    {
        Mutator mutator = stash.get(Fossil.MUTATOR, Mutator.class);

        ByteBuffer bytes = ByteBuffer.allocate(getByteSize(objects.size()));
        
        bytes.putInt(objects.size());
        bytes.putLong(next);
        for (T object : objects)
        {
            recordIO.write(bytes, object);
        }
        bytes.flip();
        mutator.write(address, bytes);
    }
}
