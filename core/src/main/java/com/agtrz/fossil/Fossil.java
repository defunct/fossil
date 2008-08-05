/* Copyright Alan Gutierrez 2006 */
package com.agtrz.fossil;

import java.io.Serializable;
import java.lang.ref.ReferenceQueue;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.agtrz.pack.Pack;
import com.agtrz.strata.Strata;
import com.agtrz.strata.Strata.Branch;
import com.agtrz.strata.Strata.Identifier;
import com.agtrz.strata.Strata.Tier;

public class Fossil<T>
implements Strata.Storage<T>, Serializable
{
    private static final int SIZEOF_INT = Integer.SIZE / Byte.SIZE;

    private static final int SIZEOF_LONG = Long.SIZE / Byte.SIZE;

    public static final int FOSSIL_LEAF_HEADER_SIZE = SIZEOF_LONG + SIZEOF_INT;

    private static final long serialVersionUID = 20070401L;

    private final Schema<T> schema;
    
    private final Strata.Store<Branch> branchStore;
    
    private final Strata.Store<T> leafStore;

    public Fossil(Strata.Structure structure, Schema<T> schema)
    {
        this.schema = schema;
        this.branchStore = new Store<Branch>(structure, new BranchReader(), new BranchWriter(), structure.getSchema().getInnerSize(), SIZEOF_LONG * 2);
        this.leafStore = new Store<T>(structure, schema.getReader(), schema.getWriter(), structure.getSchema().getLeafSize(), schema.getRecordSize());
    }

    public void commit(Object txn)
    {
        Pack.Mutator mutator = ((MutatorServer) txn).getMutator();
        mutator.commit();
    }
    
    public Strata.Store<Branch> getBranchStore()
    {
        return branchStore;
    }
    
    public Strata.Store<T> getLeafStore()
    {
        return leafStore;
    }
    
    public Strata.Storage.Schema<T> newSchema()
    {
        return new Schema<T>(schema);
    }

    final static class FossilIdentifier<T>
    implements Strata.Identifier<T>
    {
        public Object getKey(Tier<T> tier)
        {
            return tier.getStorageData();
        }

        public Object getNullKey()
        {
            return 0L;
        }

        public boolean isKeyNull(Object object)
        {
            return ((Long) object) == 0L;
        }
    }

    public final static class Store<T>
    implements Strata.Store<T>
    {
        private static final long serialVersionUID = 20080622L;

        private final Strata.Structure structure;

        private final Fossil.Reader<T> reader;

        private final Fossil.Writer<T> writer;

        private final int recordSize;

        private final int tierSize;

        private transient Map<Long, WeakMapValue<Long, Strata.Tier<T>>> mapOfTiers = new HashMap<Long, WeakMapValue<Long, Strata.Tier<T>>>();

        private transient ReferenceQueue<Strata.Tier<T>> queue = new ReferenceQueue<Strata.Tier<T>>();

        public Store(Strata.Structure structure, Fossil.Reader<T> reader, Fossil.Writer<T> writer, int tierSize, int recordSize)
        {
            this.structure = structure;
            this.reader = reader;
            this.writer = writer;
            this.tierSize = tierSize;
            this.recordSize = recordSize;
        }
        
        public Identifier<T> getIdentifier()
        {
            return new FossilIdentifier<T>();
        }

        public Strata.Tier<T> newTier(Object txn)
        {
            collect();

            Pack.Mutator mutator = ((MutatorServer) txn).getMutator();
            int blockSize = SIZEOF_INT + SIZEOF_LONG + (recordSize * tierSize);
            long address = mutator.allocate(blockSize);
            Strata.Tier<T> tier = new Strata.Tier<T>(structure, new FossilIdentifier<T>(), address, tierSize);
            mapOfTiers.put(address, new WeakMapValue<Long, Strata.Tier<T>>(address, tier, mapOfTiers, queue));

            return tier;
        }

        private synchronized void collect()
        {
            Queueable reference = null;
            while ((reference = (Queueable) queue.poll()) != null)
            {
                reference.dequeue();
            }
        }

        public Tier<T> getTier(Object txn, Object key)
        {
            collect();

            long address = (Long) key;

            if (address == 0L)
            {
                return null;
            }

            synchronized (this)
            {
                Strata.Tier<T> tier = getCached(address);

                if (tier == null)
                {
                    Pack.Mutator mutator = ((MutatorServer) txn).getMutator();
                    tier = new Strata.Tier<T>(structure, new FossilIdentifier<T>(), address, tierSize);
                    ByteBuffer in = mutator.read(address);
                    int size = in.getInt();
                    tier.setValue(in.getLong());
                    for (int i = 0; i < size; i++)
                    {
                        tier.add(reader.read(in));
                    }

                    mapOfTiers.put(address, new WeakMapValue<Long, Strata.Tier<T>>(address, tier, mapOfTiers, queue));
                }

                return tier;
            }
        }

        private Strata.Tier<T> getCached(Object key)
        {
            WeakMapValue<Long, Strata.Tier<T>> reference = mapOfTiers.get(key);
            if (reference != null)
            {
                return reference.get();
            }
            return null;
        }

        public void write(Object txn, Tier<T> tier)
        {
            Pack.Mutator mutator = ((MutatorServer) txn).getMutator();

            long address = (Long) tier.getStorageData();
            ByteBuffer out = mutator.read(address);

            out.putInt(tier.size());

            out.putLong((Long) tier.getValue());

            for (int i = 0; i < tier.size(); i++)
            {
                writer.write(out, tier.get(i));
            }

            out.flip();

            mutator.write(address, out);
        }

        public void free(Object txn, Tier<T> tier)
        {
            Pack.Mutator mutator = ((MutatorServer) txn).getMutator();
            long address = (Long) tier.getStorageData();
            mutator.free(address);
        }
    }

    public final static class Schema<T>
    implements Strata.Storage.Schema<T>, Serializable
    {
        private static final long serialVersionUID = 20071018L;

        private Reader<T> reader;

        private Writer<T> writer;

        private int recordSize;

        public Schema()
        {
            this.recordSize = 8;
        }

        public Schema(Schema<T> schema)
        {
            this.reader = schema.reader;
            this.writer = schema.writer;
            this.recordSize = schema.recordSize;
        }

        public void setReader(Reader<T> reader)
        {
            this.reader = reader;
        }

        public Reader<T> getReader()
        {
            return reader;
        }

        public void setWriter(Writer<T> writer)
        {
            this.writer = writer;
        }

        public Writer<T> getWriter()
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

        public Strata.Storage<T> newStorage(Strata.Structure structure, Object txn)
        {
            return new Fossil<T>(structure, this);
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

    public interface Reader<T>
    {
        public T read(ByteBuffer bytes);
    }

    public interface Writer<T>
    {
        public void write(ByteBuffer bytes, T object);
    }

    public final static class PackAddressWriter
    implements Writer<Long>
    {
        public void write(ByteBuffer out, Long address)
        {
            out.putLong(address);
        }
    }

    public final static class PackAddressReader
    implements Reader<Long>
    {
        public Long read(ByteBuffer in)
        {
            return in.getLong();
        }
    }

    public final static class BranchWriter
    implements Writer<Strata.Branch>
    {
        public void write(ByteBuffer bytes, Strata.Branch branch)
        {

        }
    }
    
    public final static class BranchReader
    implements Reader<Strata.Branch>
    {
        public Branch read(ByteBuffer bytes)
        {
            return null;
        }
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */
