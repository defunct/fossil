/* Copyright Alan Gutierrez 2006 */
package com.goodworkalan.fossil;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.testng.annotations.Test;

import com.goodworkalan.favorites.Stash;
import com.goodworkalan.pack.Creator;
import com.goodworkalan.pack.Mutator;
import com.goodworkalan.pack.Pack;
import com.goodworkalan.strata.Extractor;
import com.goodworkalan.strata.Schema;

public class FossilTest
{
    public final static class IntegerIO
    implements RecordIO<Integer>
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
    implements Extractor<Integer, Integer>
    {
        public Integer extract(Stash stash, Integer integer)
        {
            return integer;
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

    @Test
    public void create()
    {
        Creator creator = new Creator();
        Pack pack = creator.create(newFile());
        Mutator mutator = pack.mutate();
        Schema<Integer, Integer> schema = Fossil.newFossilSchema(new IntegerIO());
        schema.setInnerSize(5);
        schema.setLeafSize(5);
        schema.setExtractor(new IntegerExtractor());
        schema.newTransaction(Fossil.initialize(new Stash(), mutator));        
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */