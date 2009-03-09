package com.goodworkalan.fossil;

import com.goodworkalan.pack.Mutator;
import com.goodworkalan.stash.Stash;
import com.goodworkalan.strata.Store;

/**
 * A storage strategy for allocation and deallocation of tier storage from a
 * pack file.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the b+tree objects.
 */
abstract class FossilStore<T> implements Store<Long>
{
    /** The strategy to read and write an individual value object. */
    protected final RecordIO<T> recordIO;

    /**
     * Create a persistent with the given record I/O strategy.
     * 
     * @param recordIO
     *            The strategy to read and write an individual value object.
     */
    public FossilStore(RecordIO<T> recordIO)
    {
        this.recordIO = recordIO;
    }
    
    protected abstract int getByteSize(int capacity);

    /**
     * Allocate persistent storage that can hold the given number of objects
     * plus the extra tier data.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param capacity
     *            The number of objects to store.
     * @return The address of the persistent storage.
     */
    public Long allocate(Stash stash, int capacity)
    {
        Mutator mutator = stash.get(Fossil.MUTATOR, Mutator.class);
        return mutator.allocate(getByteSize(capacity));
    }
    

    /**
     * Free the persistent storage at the given address.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param address
     *            The address to free.
     */
    public void free(Stash stash, Long address)
    {
        Mutator mutator = stash.get(Fossil.MUTATOR, Mutator.class);
        mutator.free(address);
    }
}
