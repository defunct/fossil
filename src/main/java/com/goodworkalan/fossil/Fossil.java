/* Copyright Alan Gutierrez 2006 */
package com.goodworkalan.fossil;

import com.goodworkalan.stash.Stash;
import com.goodworkalan.pack.Mutator;
import com.goodworkalan.strata.Schema;
import com.goodworkalan.strata.Stratas;

// TODO Document.
public class Fossil
{
    // TODO Document.
    final static Stash.Key MUTATOR = new Stash.Key();
    
    // TODO Document.
    final static int SIZEOF_SHORT = Short.SIZE / Byte.SIZE;
    
    // TODO Document.
    final static int SIZEOF_INTEGER = Integer.SIZE / Byte.SIZE;
    
    // TODO Document.
    final static int SIZEOF_LONG = Long.SIZE / Byte.SIZE;

    // TODO Document.
    public static Stash initialize(Stash stash, Mutator mutator)
    {
        stash.put(MUTATOR, Mutator.class, mutator);
        return stash;
    }
    
    // TODO Document.
    public static <T, F extends Comparable<? super F>> Schema<T, F> newFossilSchema()
    {
        Schema<T, F> schema = new Schema<T, F>();
        schema.setAllocatorBuilder(Stratas.newStorageAllocatorBuilder());
        schema.setFieldCaching(true);
        schema.setTierPoolBuilder(Stratas.newBasicTierPool());
        schema.setTierWriterBuilder(Stratas.newPerQueryTierWriter(8));
        return schema;
    }
}

/* vim: set et sw=4 ts=4 ai tw=80 nowrap: */