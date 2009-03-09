package com.goodworkalan.fossil;

import com.goodworkalan.strata.InnerStore;
import com.goodworkalan.strata.LeafStore;
import com.goodworkalan.strata.Storage;

/**
 * A persistent storage strategy for strata b-trees that writes to a pack file
 * with the given record I/O strategy.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the b+tree objects.
 */
public class FossilStorage<T> implements Storage<T, Long>
{
    /** The storage strategy for inner tier branches. */
    private final InnerStore<T, Long> innerStore;
    
    /** The storage strategy for leaf tier value objects. */
    private final LeafStore<T, Long> leafStore;

    /**
     * Create a persistent storage strategy for strata b-trees that writes to a
     * pack file with the given record I/O strategy.
     * 
     * @param recordIO
     *            The strategy to read and write an individual value object.
     */
    public FossilStorage(RecordIO<T> recordIO)
    {
        this.innerStore = new FossilInnerStore<T>(recordIO);
        this.leafStore = new FossilLeafStore<T>(recordIO);
    }

    /**
     * Get the storage strategy for inner tiers.
     * 
     * @return The storage strategy for inner tiers.
     */
    public InnerStore<T, Long> getInnerStore()
    {
        return innerStore;
    }
    
    /**
     * Get the storage strategy for leaf tiers.
     * 
     * @return The storage strategy for leaf tiers.
     */
    public LeafStore<T, Long> getLeafStore()
    {
        return leafStore;
    }

    /**
     * Get the null address value for this allocation strategy.
     * 
     * @return The null address value.
     */
    public Long getNull()
    {
        return 0L;
    }
    
    /**
     * Return true if the given address is the null value for this allocation
     * strategy.
     * 
     * @param address
     *            A storage address.
     * @return True if the address is null.
     */
    public boolean isNull(Long address)
    {
        return address == 0L;
    }
}
