/* Copyright Alan Gutierrez 2006 */
package com.agtrz.fossil;

import java.nio.ByteBuffer;

import com.agtrz.pack.Pack;
import com.agtrz.strata.Strata;
import com.agtrz.strata.Strata.Record;

public class FossilTest
{
    private final static class IntegerIO
    implements Fossil.RecordIO<Integer>
    {
        public void write(ByteBuffer bytes, Integer object)
        {
            bytes.putInt(object);
        }
        
        public Integer read(ByteBuffer bytes)
        {
            return bytes.getInt();
        }
        
        public int getSize()
        {
            return Integer.SIZE / Byte.SIZE;
        }
    }
    
    private final static class IntegerExtractor
    implements Strata.Extractor<Integer, Pack.Mutator>
    {
        public void extract(Pack.Mutator mutator, Integer integer, Record record)
        {
            record.fields(integer);
        }
    }

    public void create()
    {
        Strata.StrataBuilder builder = new Strata.StrataBuilder();
        builder.newTree(new Fossil.Storage<Integer>(new IntegerIO()), new IntegerExtractor());
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */