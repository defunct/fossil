/* Copyright Alan Gutierrez 2006 */
package com.agtrz.fossil;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.Map;

public class SoftMapValue<K, V>
extends SoftReference<V> implements Queueable
{
    private final Map<K, SoftMapValue<K, V>> map;

    private final K key;

    public SoftMapValue(K key, V value, Map<K, SoftMapValue<K, V>> map, ReferenceQueue<V> queue)
    {
        super(value, queue);
        this.map = map;
        this.key = key;
    }

    public void dequeue()
    {
        map.remove(key);
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */