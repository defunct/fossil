/* Copyright Alan Gutierrez 2006 */
package com.agtrz.fossil;

import java.nio.ByteBuffer;

import com.agtrz.pack.Pack;
import com.agtrz.pack.Pack.Mutator;
import com.agtrz.strata.Strata;

public class Fossil
{
    private final static int SIZEOF_SHORT = Short.SIZE / Byte.SIZE;
    
    private final static int SIZEOF_INTEGER = Integer.SIZE / Byte.SIZE;
    
    private final static int SIZEOF_LONG = Long.SIZE / Byte.SIZE;

    public final static class LeafStore<T>
    implements Strata.LeafStore<T, Long, Pack.Mutator>
    {
        private final RecordIO<T> recordIO;
        
        public LeafStore(RecordIO<T> recordIO)
        {
            this.recordIO = recordIO;
        }
        
        private int getSize(int size)
        {
            return SIZEOF_INTEGER + SIZEOF_LONG + (size * recordIO.getSize());
        }
        
        public Long allocate(Pack.Mutator mutator, int size)
        {
            return mutator.allocate(size);
        }

        // FIXME Load into a leaf tier, do not return one.
        public <B> Strata.LeafTier<B, Long> load(Pack.Mutator mutator, Long address, Strata.Cooper<T, B, Mutator> cooper, Strata.Extractor<T, Pack.Mutator> extractor)
        {
            Strata.LeafTier<B, Long> leaf = new Strata.LeafTier<B, Long>();

            ByteBuffer bytes = mutator.read(address);
            int size = bytes.getInt();

            leaf.setNext(bytes.getLong());
            
            for (int i = 0; i < size; i++)
            {
                T object = recordIO.read(bytes);
                B bucket = cooper.newBucket(mutator, extractor, object);
                leaf.add(bucket);
            }
            
            return leaf;
        }
        
        public <B> void write(Pack.Mutator mutator, Strata.LeafTier<B, Long> leaf, Strata.Cooper<T, B, Mutator> cooper, Strata.Extractor<T, Pack.Mutator> extractor)
        {
            ByteBuffer bytes = ByteBuffer.allocate(getSize(leaf.size()));
            
            bytes.putInt(leaf.size());
            bytes.putLong(leaf.getNext());
            for (int i = 0; i < leaf.size(); i++)
            {
                recordIO.write(bytes, cooper.getObject(leaf.get(i)));
            }
            bytes.flip();
            mutator.write(leaf.getAddress(), bytes);
        }
        
        public void free(Pack.Mutator mutator, Long address)
        {
            mutator.free(address);
        }
    }
    
    public final static class InnerStore<T>
    implements Strata.InnerStore<T, Long, Pack.Mutator>
    {    
        private final RecordIO<T> recordIO;
        
        public InnerStore(RecordIO<T> recordIO)
        {
            this.recordIO = recordIO;
        }
        
        private int getSize(int size)
        {
            return SIZEOF_INTEGER + SIZEOF_SHORT + (size * (recordIO.getSize() + SIZEOF_LONG));
        }

        public Long allocate(Pack.Mutator mutator, int size)
        {
            return mutator.allocate(getSize(size));
        }
        
        public <B> Strata.InnerTier<B, Long> load(Pack.Mutator mutator, Long address, Strata.Cooper<T, B, Mutator> cooper, Strata.Extractor<T, Mutator> extractor)
        {
            Strata.InnerTier<B, Long> inner = new Strata.InnerTier<B, Long>();
            ByteBuffer bytes = mutator.read(address);
            int size = bytes.getInt();
            short type = bytes.getShort();
            inner.setChildType(type == 1 ? Strata.ChildType.INNER : Strata.ChildType.LEAF);
            for (int i = 0; i < size; i++)
            {
                T object = recordIO.read(bytes);
                Long childAddress = bytes.getLong();
                B bucket = cooper.newBucket(mutator, extractor, object);
                inner.add(new Strata.Branch<B, Long>(bucket, childAddress));
            }
            return inner;
        }
        
        public <B> void write(Pack.Mutator mutator, Strata.InnerTier<B, Long> inner, Strata.Cooper<T, B, Mutator> cooper, Strata.Extractor<T, Mutator> extractor)
        {
            ByteBuffer bytes = ByteBuffer.allocate(getSize(inner.size()));
            bytes.putInt(inner.size());
            bytes.putShort(inner.getChildType() == Strata.ChildType.INNER ? (short) 1 : (short) 2);
            for (int i = 0; i < inner.size(); i++)
            {
                T object = cooper.getObject(inner.get(i).getPivot());
                recordIO.write(bytes, object);
                bytes.putLong(inner.get(i).getAddress());
            }
            bytes.flip();
            mutator.write(inner.getAddress(), bytes);
        }
        
        public void free(Pack.Mutator mutator, Long address)
        {
            mutator.free(address);
        }
    }

    public final static class Storage<T>
    implements Strata.Storage<T, Long, Pack.Mutator>
    {
        private final Strata.InnerStore<T, Long, Pack.Mutator> innerStore;
        
        private final Strata.LeafStore<T, Long, Pack.Mutator> leafStore;
        
        public Storage(RecordIO<T> recordIO)
        {
            this.innerStore = new InnerStore<T>(recordIO);
            this.leafStore = new LeafStore<T>(recordIO);
        }
        
        public Strata.InnerStore<T, Long, Pack.Mutator> getInnerStore()
        {
            return innerStore;
        }
        
        public Strata.LeafStore<T, Long, Pack.Mutator> getLeafStore()
        {
            return leafStore;
        }

        public void commit(Pack.Mutator mutator)
        {
            mutator.commit();
        }
        
        public Long getNull()
        {
            return 0L;
        }
        
        public boolean isNull(Long address)
        {
            return address == 0L;
        }
    }
    
    public interface RecordIO<T>
    {
        public T read(ByteBuffer bytes);
        
        public void write(ByteBuffer bytes, T object);
        
        public int getSize();
    }
    
    private final static class StorageBuilder<T>
    implements Strata.StorageBuilder<T, Pack.Mutator>
    {
        private final RecordIO<T> recordIO;
        
        public StorageBuilder(RecordIO<T> recordIO)
        {
            this.recordIO = recordIO;
        }

        public Strata.Transaction<T, Pack.Mutator> newTransaction(Pack.Mutator txn, Strata.Schema<T, Pack.Mutator> schema)
        {
            return schema.newTransaction(txn, new Storage<T>(recordIO));
        }
    }
    
    public static <T> Strata.Schema<T, Pack.Mutator> newFossilSchema(RecordIO<T> recordIO)
    {
        Strata.Schema<T, Pack.Mutator> schema = new Strata.Schema<T, Pack.Mutator>();
        schema.setStorageBuilder(new StorageBuilder<T>(recordIO));
        schema.setAllocatorBuilder(Strata.newStorageAllocatorBuilder());
        schema.setFieldCaching(true);
        schema.setTierPoolBuilder(Strata.newBasicTierPool());
        schema.setTierWriterBuilder(Strata.newPerQueryTierWriter(8));
        return schema;
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */