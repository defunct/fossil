/* Copyright Alan Gutierrez 2006 */
package com.agtrz.fossil;

import java.lang.ref.ReferenceQueue;
import java.util.HashMap;
import java.util.Map;

import com.agtrz.strata.Strata;
import com.agtrz.swag.util.WeakMapValue;

/**
 * This class is the base of {@link BentoStorage} and is necessary to
 * initialize the transient fields during deserialization.
 * 
 * @author Alan Gutierrez
 */
class BentoStorageBase
{
    protected final transient ReferenceQueue<Strata.Tier> queue = new ReferenceQueue<Strata.Tier>();

    protected final transient Map<Long, WeakMapValue<Long, Strata.Tier>> mapOfTiers = new HashMap<Long, WeakMapValue<Long, Strata.Tier>>();
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */