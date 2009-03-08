package com.goodworkalan.fossil;

import com.goodworkalan.strata.Allocator;
import com.goodworkalan.strata.Storage;
import com.goodworkalan.strata.TierPool;

// TODO Document.
public final class FossilStorage<T>
implements Storage<T, Long>
{
    // TODO Document.
    private final RecordIO<T> recordIO;

    // TODO Document.
    public FossilStorage(RecordIO<T> recordIO)
    {
        this.recordIO = recordIO;
    }
    
    public Allocator<T, Long> getAllocator()
    {
        return new FossilAllocator<T>(recordIO);
    }
    
    public TierPool<T, Long> getTierPool()
    {
        return null;
    }
}