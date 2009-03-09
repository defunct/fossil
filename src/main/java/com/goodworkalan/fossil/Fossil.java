/* Copyright Alan Gutierrez 2006 */
package com.goodworkalan.fossil;

import com.goodworkalan.pack.Mutator;
import com.goodworkalan.stash.Stash;

/**
 * Static methods to initiate the type-safe container of out of band data with
 * fossil specific data.
 * 
 * @author Alan Gutierrez
 * 
 */
public class Fossil
{
    /**
     * The key to of the mutator in the type-safe collection of out of band
     * data.
     */
    final static Stash.Key MUTATOR = new Stash.Key();
    
    /** The size in bytes of a primitive short. */
    final static int SIZEOF_SHORT = Short.SIZE / Byte.SIZE;
    
    /** The size in bytes of a primitive integer. */
    final static int SIZEOF_INTEGER = Integer.SIZE / Byte.SIZE;
    
    /** The size in bytes of a primitive long. */
    final static int SIZEOF_LONG = Long.SIZE / Byte.SIZE;

    /**
     * Add the given mutator to the given type-safe container of out of band
     * data so that it can be retrieved by the classes in the fossil package.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param mutator
     *            A pack file mutator.
     * @return The given type-safe container of out of band data.
     */
    public static Stash initialize(Stash stash, Mutator mutator)
    {
        stash.put(MUTATOR, Mutator.class, mutator);
        return stash;
    }

    /**
     * Create type-safe container of out of band data and add the given mutator
     * so that it can be retrieved by the classes in the fossil package.
     * 
     * @param mutator
     *            A pack file mutator.
     * @return The a new type-safe container of out of band data containing the
     *         mutator.
     */
    public static Stash newStash(Mutator mutator)
    {
        return initialize(new Stash(), mutator);
    }
}

/* vim: set et sw=4 ts=4 ai tw=80 nowrap: */