package com.goodworkalan.fossil;

import com.goodworkalan.pack.Mutator;
import com.goodworkalan.strata.InnerStore;
import com.goodworkalan.strata.LeafStore;
import com.goodworkalan.strata.Storage;

public final class FossilStorage<T>
implements Storage<T, Long, Mutator>
{
    private final InnerStore<T, Long, Mutator> innerStore;
    
    private final LeafStore<T, Long, Mutator> leafStore;
    
    public FossilStorage(RecordIO<T> recordIO)
    {
        this.innerStore = new InnerFossil<T>(recordIO);
        this.leafStore = new FossilLeafStore<T>(recordIO);
    }
    
    public InnerStore<T, Long, Mutator> getInnerStore()
    {
        return innerStore;
    }
    
    public LeafStore<T, Long, Mutator> getLeafStore()
    {
        return leafStore;
    }

    public void commit(Mutator mutator)
    {
        mutator.commit();
    }
    
    public Long getNull()
    {
        return 0L;
    }
    
    public boolean isNull(Long address)
    {
        return address == 0L;
    }
}