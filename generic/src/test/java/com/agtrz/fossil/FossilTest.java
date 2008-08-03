/* Copyright Alan Gutierrez 2006 */
package com.agtrz.fossil;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.agtrz.pack.Pack;
import com.agtrz.strata.Strata;

public class FossilTest
{
    public final static class IntegerIO
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
    
    public final static class IntegerExtractor
    implements Strata.Extractor<Integer, Pack.Mutator>
    {
        public void extract(Pack.Mutator mutator, Integer integer, Strata.Record record)
        {
            record.fields(integer);
        }
    }
    
    private File newFile()
    {
        File file;
        try
        {
            file = File.createTempFile("strata", ".sta");
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        file.deleteOnExit();
        return file;
    }

    public void create()
    {
        Pack.Creator creator = new Pack.Creator();
        Pack pack = creator.create(newFile());
        Pack.Mutator mutator = pack.mutate();
        Strata.Schema<Integer, Pack.Mutator> schema = Fossil.newFossilSchema(new IntegerIO());
        schema.setInnerSize(5);
        schema.setLeafSize(5);
        schema.setExtractor(new IntegerExtractor());
        schema.newTransaction(mutator);        
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */