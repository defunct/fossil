/* Copyright Alan Gutierrez 2006 */
package com.agtrz.fossil;

import java.io.Serializable;
import java.nio.ByteBuffer;

import com.agtrz.pack.Pack;
import com.agtrz.strata.Strata;
import com.agtrz.strata.Strata.Storage;
import com.agtrz.swag.util.Queueable;
import com.agtrz.swag.util.WeakMapValue;

public class BentoStorage
extends BentoStorageBase
implements Strata.Storage, Serializable
{
    private static final int SIZEOF_SHORT = Short.SIZE / Byte.SIZE;

    private static final int SIZEOF_INT = Integer.SIZE / Byte.SIZE;
    
    private static final int SIZEOF_LONG = Long.SIZE / Byte.SIZE;
    
    private static final long serialVersionUID = 20070401L;

    private final Schema schema;

    public BentoStorage(Schema schema)
    {
        this.schema = schema;
    }

    public Strata.InnerTier newInnerTier(Strata.Structure structure, Object txn, short typeOfChildren)
    {
        Pack.Mutator mutator = ((MutatorServer) txn).getMutator();
        int blockSize = SIZEOF_SHORT + SIZEOF_INT + (SIZEOF_LONG + schema.getRecordSize()) * (structure.getSchema().getSize() + 1);
        long address = mutator.allocate(blockSize);
        Strata.InnerTier inner = new Strata.InnerTier(structure, address, typeOfChildren);
        mapOfTiers.put(address, new WeakMapValue<Long, Strata.Tier>(address, inner, mapOfTiers, queue));
        return inner;
    }

    public Strata.LeafTier newLeafTier(Strata.Structure structure, Object txn)
    {
        Pack.Mutator mutator = ((MutatorServer) txn).getMutator();
        int blockSize = SIZEOF_INT + (SIZEOF_LONG * 2) + (schema.getRecordSize() * structure.getSchema().getSize());
        long address = mutator.allocate(blockSize);
        Strata.LeafTier leaf = new Strata.LeafTier(structure, address);
        leaf.setNextLeafKey(Pack.NULL_ADDRESS);
        mapOfTiers.put(address, new WeakMapValue<Long, Strata.Tier>(address, leaf, mapOfTiers, queue));
        return leaf;
    }

    public Strata.InnerTier getInnerTier(Strata.Structure structure, Object txn, Object key)
    {
        collect();

        long address = (Long) key;

        if (address == 0L)
        {
            return null;
        }

        synchronized (this)
        {
            Strata.InnerTier inner = (Strata.InnerTier) getCached(address);

            if (inner == null)
            {
                Pack.Mutator mutator = ((MutatorServer) txn).getMutator();

                ByteBuffer in = mutator.read(address);
                short typeOfChildren = in.getShort();

                inner = new Strata.InnerTier(structure, key, typeOfChildren);

                int size = in.getInt();
                if (size != 0)
                {
                    long keyOfTier = in.getLong();
                    inner.add(txn, keyOfTier, null);
                    for (int j = 0; j < schema.getRecordSize(); j++)
                    {
                        in.get();
                    }
                }
                for (int i = 1; i < size; i++)
                {
                    long keyOfTier = in.getLong();
                    Object object = schema.getReader().read(in);
                    inner.add(txn, keyOfTier, object);
                }

                mapOfTiers.put(address, new WeakMapValue<Long, Strata.Tier>(address, inner, mapOfTiers, queue));
            }

            return inner;
        }
    }

    public Strata.LeafTier getLeafTier(Strata.Structure structure, Object txn, Object key)
    {
        collect();

        long address = (Long) key;

        if (address == 0L)
        {
            return null;
        }

        synchronized (this)
        {
            Strata.LeafTier leaf = (Strata.LeafTier) getCached(address);

            if (leaf == null)
            {
                Pack.Mutator mutator = ((MutatorServer) txn).getMutator();
                leaf = new Strata.LeafTier(structure, address);
                ByteBuffer in = mutator.read(address);
                int size = in.getInt();
                leaf.setNextLeafKey(in.getLong());
                for (int i = 0; i < size; i++)
                {
                    leaf.add(txn, schema.getReader().read(in));
                }

                mapOfTiers.put(address, new WeakMapValue<Long, Strata.Tier>(address, leaf, mapOfTiers, queue));
            }

            return leaf;
        }
    }

    public void write(Strata.Structure structure, Object txn, Strata.InnerTier inner)
    {
        Pack.Mutator mutator = ((MutatorServer) txn).getMutator();

        long address = (Long) inner.getStorageData();
        ByteBuffer out = mutator.read(address);

        out.putShort(inner.getChildType());
        out.putInt(inner.getSize() + 1);

        for (int i = 0; i < inner.getSize() + 1; i++)
        {
            Strata.Branch branch = inner.get(i);

            if (!(branch.getRightKey() instanceof Long))
            {
                System.out.println(branch.getRightKey());
            }

            long addressOfChild = (Long) branch.getRightKey();
            out.putLong(addressOfChild);

            if (branch.isMinimal())
            {
                for (int j = 0; j < schema.getRecordSize(); j++)
                {
                    out.put((byte) 0);
                }
            }
            else
            {
                schema.getWriter().write(out, branch.getPivot());
            }
        }

        // for (int i = inner.getSize() + 1; i < structure.getSize() + 1; i++)
        // {
        // out.putLong(0L);
        // out.putInt(0);
        //
        // for (int j = 0; j < recordSize; j++)
        // {
        // out.put((byte) 0);
        // }
        // }

        out.clear();
        
        mutator.write(address, out);
    }

    public void write(Strata.Structure structure, Object txn, Strata.LeafTier leaf)
    {
        Pack.Mutator mutator = ((MutatorServer) txn).getMutator();

        long address = (Long) leaf.getStorageData();
        ByteBuffer out = mutator.read(address);
        
        out.putInt(leaf.getSize());

        long addressOfNext = (Long) leaf.getNextLeafKey();
        out.putLong(addressOfNext);

        for (int i = 0; i < leaf.getSize(); i++)
        {
            schema.getWriter().write(out, structure.getObjectKey(leaf.get(i)));
        }

        // for (int i = leaf.getSize(); i < structure.getSize(); i++)
        // {
        // for (int j = 0; j < recordSize; j++)
        // {
        // out.put((byte) 0);
        // }
        // }

        out.flip();
        
        mutator.write(address, out);
    }

    public void free(Strata.Structure structure, Object txn, Strata.InnerTier inner)
    {
        Pack.Mutator mutator = ((MutatorServer) txn).getMutator();
        long address = (Long) inner.getStorageData();
        mutator.free(address);
    }

    public void free(Strata.Structure structure, Object txn, Strata.LeafTier leaf)
    {
        Pack.Mutator mutator = ((MutatorServer) txn).getMutator();
        long address = (Long) leaf.getStorageData();
        mutator.free(address);
    }

    public void commit(Object txn)
    {
        Pack.Mutator mutator = ((MutatorServer) txn).getMutator();
        mutator.commit();
    }

    public Object getKey(Strata.Tier leaf)
    {
        return leaf.getStorageData();
    }

    public Object getNullKey()
    {
        return Pack.NULL_ADDRESS;
    }

    public boolean isKeyNull(Object object)
    {
        return Pack.NULL_ADDRESS == (Long) object;
    }

    public Strata.Storage.Schema getSchema()
    {
        return new Schema(schema);
    }

    private synchronized void collect()
    {
        Queueable reference = null;
        while ((reference = (Queueable) queue.poll()) != null)
        {
            reference.dequeue();
        }
    }

    private Strata.Tier getCached(Object key)
    {
        WeakMapValue<Long, Strata.Tier> reference = mapOfTiers.get(key);
        if (reference != null)
        {
            return reference.get();
        }
        return null;
    }

    public final static class Schema
    implements Strata.Storage.Schema, Serializable
    {
        private static final long serialVersionUID = 20071018L;

        private Reader reader;

        private Writer writer;

        private int recordSize;

        public Schema()
        {
            this.reader = new BentoAddressReader();
            this.writer = new BentoAddressWriter();
            this.recordSize = 8;
        }

        public Schema(Schema schema)
        {
            this.reader = schema.reader;
            this.writer = schema.writer;
            this.recordSize = schema.recordSize;
        }

        public void setReader(Reader reader)
        {
            this.reader = reader;
        }

        public Reader getReader()
        {
            return reader;
        }

        public void setWriter(Writer writer)
        {
            this.writer = writer;
        }

        public Writer getWriter()
        {
            return writer;
        }

        public int getRecordSize()
        {
            return recordSize;
        }

        public void setSize(int recordSize)
        {
            this.recordSize = recordSize;
        }

        public Storage newStorage()
        {
            return new BentoStorage(this);
        }
    }

    public static MutatorServer txn(Pack.Mutator mutator)
    {
        return new BasicMutatorServer(mutator);
    }

    public interface MutatorServer
    {
        Pack.Mutator getMutator();
    }

    public final static class BasicMutatorServer
    implements MutatorServer
    {
        private Pack.Mutator mutator;

        public BasicMutatorServer(Pack.Mutator mutator)
        {
            this.mutator = mutator;
        }

        public Pack.Mutator getMutator()
        {
            return mutator;
        }
    }

    public interface Reader
    {
        public Object read(ByteBuffer bytes);
    }

    public interface Writer
    {
        public void write(ByteBuffer bytes, Object object);
    }

    public final static class BentoAddressWriter
    implements Writer
    {
        public void write(ByteBuffer out, Object object)
        {
            long address = ((Long) object).longValue();
            out.putLong(address);
        }
    }

    public final static class BentoAddressReader
    implements Reader
    {
        public Object read(ByteBuffer in)
        {
            return new Long(in.getLong());
        }
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */
