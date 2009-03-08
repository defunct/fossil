/* Copyright Alan Gutierrez 2006 */
package com.goodworkalan.fossil;

import com.goodworkalan.pack.Mutator;
import com.goodworkalan.stash.Stash;

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
    
    public static Stash newStash(Mutator mutator)
    {
        return initialize(new Stash(), mutator);
    }
}

/* vim: set et sw=4 ts=4 ai tw=80 nowrap: */