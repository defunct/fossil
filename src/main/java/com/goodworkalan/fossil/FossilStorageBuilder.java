package com.goodworkalan.fossil;

import com.goodworkalan.stash.Stash;
import com.goodworkalan.strata.Query;
import com.goodworkalan.strata.Schema;
import com.goodworkalan.strata.StorageBuilder;

public class FossilStorageBuilder<T, F extends Comparable<? super F>>
implements StorageBuilder<T, F>
{
    private final RecordIO<T> recordIO;
    
    public FossilStorageBuilder(RecordIO<T> recordIO)
    {
        this.recordIO = recordIO;
    }

    public Query<T, F> create(Stash stash, Schema<T, F> schema)
    {
        return schema.create(stash, new FossilStorage<T, F>(recordIO)).getQuery();
    }
}