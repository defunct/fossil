/* Copyright Alan Gutierrez 2006 */
package com.goodworkalan.fossil;

import com.goodworkalan.pack.Mutator;
import com.goodworkalan.strata.Schema;
import com.goodworkalan.strata.Strata;

public class Fossil
{
    final static int SIZEOF_SHORT = Short.SIZE / Byte.SIZE;
    
    final static int SIZEOF_INTEGER = Integer.SIZE / Byte.SIZE;
    
    final static int SIZEOF_LONG = Long.SIZE / Byte.SIZE;

    
    public static <T> Schema<T, Mutator> newFossilSchema(RecordIO<T> recordIO)
    {
        Schema<T, Mutator> schema = new Schema<T, Mutator>();
        schema.setStorageBuilder(new FossilStorageBuilder<T>(recordIO));
        schema.setAllocatorBuilder(Strata.newStorageAllocatorBuilder());
        schema.setFieldCaching(true);
        schema.setTierPoolBuilder(Strata.newBasicTierPool());
        schema.setTierWriterBuilder(Strata.newPerQueryTierWriter(8));
        return schema;
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */