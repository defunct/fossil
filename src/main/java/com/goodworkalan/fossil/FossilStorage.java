package com.goodworkalan.fossil;

import com.goodworkalan.stash.Stash;
import com.goodworkalan.pack.Mutator;
import com.goodworkalan.strata.InnerStore;
import com.goodworkalan.strata.LeafStore;
import com.goodworkalan.strata.Storage;

public final class FossilStorage<T, F extends Comparable<? super F>>
implements Storage<T, F, Long>
{
    private final InnerStore<T, F, Long> innerStore;
    
    private final LeafStore<T, F, Long> leafStore;
    
    public FossilStorage(RecordIO<T> recordIO)
    {
        this.innerStore = new FossilInnerStore<T, F>(recordIO);
        this.leafStore = new FossilLeafStore<T, F>(recordIO);
    }
    
    public InnerStore<T, F, Long> getInnerStore()
    {
        return innerStore;
    }
    
    public LeafStore<T, F, Long> getLeafStore()
    {
        return leafStore;
    }

    public void commit(Stash stash)
    {
        Mutator mutator = stash.get(Fossil.MUTATOR, Mutator.class);
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