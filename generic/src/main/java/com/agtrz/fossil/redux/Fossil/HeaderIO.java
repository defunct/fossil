/* Copyright Alan Gutierrez 2006 */
package com.agtrz.fossil.redux.Fossil;

import java.nio.ByteBuffer;

public interface HeaderIO<H>
{
    public H read(ByteBuffer bytes);
    
    public void write(ByteBuffer bytes, H header);
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */