package com.goodworkalan.fossil;

import com.goodworkalan.pack.Mutator;
import com.goodworkalan.strata.Schema;
import com.goodworkalan.strata.StorageBuilder;
import com.goodworkalan.strata.Transaction;

public class FossilStorageBuilder<T>
implements StorageBuilder<T, Mutator>
{
    private final RecordIO<T> recordIO;
    
    public FossilStorageBuilder(RecordIO<T> recordIO)
    {
        this.recordIO = recordIO;
    }

    public Transaction<T, Mutator> newTransaction(Mutator txn, Schema<T, Mutator> schema)
    {
        return schema.newTransaction(txn, new FossilStorage<T>(recordIO));
    }
}