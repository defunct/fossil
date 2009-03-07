package com.goodworkalan.fossil;

import com.goodworkalan.stash.Stash;
import com.goodworkalan.pack.Mutator;
import com.goodworkalan.strata.InnerStore;
import com.goodworkalan.strata.LeafStore;
import com.goodworkalan.strata.Storage;

// TODO Document.
public final class FossilStorage<T, F extends Comparable<? super F>>
implements Storage<T, F, Long>
{
    // TODO Document.
    private final InnerStore<T, F, Long> innerStore;
    
    // TODO Document.
    private final LeafStore<T, F, Long> leafStore;
    
    // TODO Document.
    public FossilStorage(RecordIO<T> recordIO)
    {
        this.innerStore = new FossilInnerStore<T, F>(recordIO);
        this.leafStore = new FossilLeafStore<T, F>(recordIO);
    }
    
    // TODO Document.
    public InnerStore<T, F, Long> getInnerStore()
    {
        return innerStore;
    }
    
    // TODO Document.
    public LeafStore<T, F, Long> getLeafStore()
    {
        return leafStore;
    }

    // TODO Document.
    public void commit(Stash stash)
    {
        Mutator mutator = stash.get(Fossil.MUTATOR, Mutator.class);
        mutator.commit();
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