/**
 * File: $HeadURL: https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/dictionary/impl/section/PFCDictionarySection.java $
 * Revision: $Rev: 94 $
 * Last modified: $Date: 2012-11-20 23:44:36 +0000 (mar, 20 nov 2012) $
 * Last modified by: $Author: mario.arias $
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 *
 * Contacting the authors:
 *   Mario Arias:               mario.arias@deri.org
 *   Javier D. Fernandez:       jfergar@infor.uva.es
 *   Miguel A. Martinez-Prieto: migumar2@infor.uva.es
 *   Alejandro Andres:          fuzzy.alej@gmail.com
 */

package org.rdfhdt.hdt.dictionary.impl.section;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Iterator;

import org.rdfhdt.hdt.compact.integer.VByte;
import org.rdfhdt.hdt.compact.sequence.Sequence;
import org.rdfhdt.hdt.compact.sequence.SequenceFactory;
import org.rdfhdt.hdt.dictionary.DictionarySectionPrivate;
import org.rdfhdt.hdt.dictionary.TempDictionarySection;
import org.rdfhdt.hdt.exceptions.CRCException;
import org.rdfhdt.hdt.exceptions.IllegalFormatException;
import org.rdfhdt.hdt.exceptions.NotImplementedException;
import org.rdfhdt.hdt.listener.ProgressListener;
import org.rdfhdt.hdt.util.crc.CRC8;
import org.rdfhdt.hdt.util.crc.CRCInputStream;
import org.rdfhdt.hdt.util.io.CountInputStream;
import org.rdfhdt.hdt.util.io.IOUtil;
import org.rdfhdt.hdt.util.string.ByteStringUtil;
import org.rdfhdt.hdt.util.string.ReplazableString;

/**
 * @author mario.arias
 *
 */
public class PFCDictionarySectionMap implements DictionarySectionPrivate,Closeable {
	public static final int TYPE_INDEX = 2;
	public static final int DEFAULT_BLOCK_SIZE = 16;
	
	private static final int BLOCKS_PER_BYTEBUFFER = 1000000;
	protected FileChannel ch;
	protected ByteBuffer [] buffers; // Encoded sequence
	long [] posFirst;	// Global byte position of the start of each buffer
	protected int blocksize;
	protected int numstrings;
	protected Sequence blocks;
	protected long dataSize;
	
	private ThreadLocal<ByteBuffer[]> cacheBuffers =
			new ThreadLocal<ByteBuffer[]>() {
				protected ByteBuffer[] initialValue() {
					ByteBuffer [] newBuf = new ByteBuffer[buffers.length];
					for(int i=0;i<buffers.length;i++) {
						newBuf[i] = buffers[i].duplicate();
						newBuf[i].order(ByteOrder.LITTLE_ENDIAN);
					}
					return newBuf;
				};
	};
	
	public PFCDictionarySectionMap(CountInputStream input, File f) throws IOException {
	
		CRCInputStream crcin = new CRCInputStream(input, new CRC8());
		
		// Read type
		int type = crcin.read();
		if(type!=TYPE_INDEX) {
			throw new IllegalFormatException("Trying to read a DictionarySectionPFC from data that is not of the suitable type");
		}
		
		// Read vars
		numstrings = (int) VByte.decode(crcin);
		dataSize = VByte.decode(crcin);
		blocksize = (int) VByte.decode(crcin);		
	
		if(!crcin.readCRCAndCheck()) {
			throw new CRCException("CRC Error while reading Dictionary Section Plain Front Coding Header.");
		}
		
		// Read blocks
//		blocks = SequenceFactory.createStream(input, f);
		blocks = SequenceFactory.createStream(input);
		blocks.load(input, null);
		
		long base = input.getTotalBytes();
		IOUtil.skip(crcin, dataSize+4); // Including CRC32
		
		// Read packed data
		ch = new FileInputStream(f).getChannel();
		int block = 0;
		int buffer = 0;
		long numBlocks = blocks.getNumberOfElements();
		long bytePos = 0;
		long numBuffers = 1+numBlocks/BLOCKS_PER_BYTEBUFFER;
		buffers = new ByteBuffer[(int)numBuffers ];
		posFirst = new long[(int)numBuffers];
		
//		System.out.println("Buffers "+buffers.length);
		while(block<numBlocks-1) {
			int nextBlock = (int) Math.min(numBlocks-1, block+BLOCKS_PER_BYTEBUFFER);
			long nextBytePos = blocks.get(nextBlock);
			
//			System.out.println("From block "+block+" to "+nextBlock);
//			System.out.println("From pos "+ bytePos+" to "+nextBytePos);
//			System.out.println("Total size: "+ (nextBytePos-bytePos));
			buffers[buffer] = ch.map(MapMode.READ_ONLY, base+bytePos, nextBytePos-bytePos);
			buffers[buffer].order(ByteOrder.LITTLE_ENDIAN);
			
			posFirst[buffer] = bytePos;
			
			bytePos = nextBytePos;
			block+=BLOCKS_PER_BYTEBUFFER;
			buffer++;
		}
	}

	private int locateBlock(CharSequence str) {
		if(blocks.getNumberOfElements()==0) {
			return -1;
		}
		
		int low = 0;
		int high = (int)blocks.getNumberOfElements()-1;
		
		while (low <= high) {
			int mid = (low + high) >>> 1;
		
			ByteBuffer buffer = cacheBuffers.get()[mid/BLOCKS_PER_BYTEBUFFER];
			buffer.position((int)(blocks.get(mid)-posFirst[mid/BLOCKS_PER_BYTEBUFFER]));

			int cmp = ByteStringUtil.strcmp(str, buffer);
			
//			buffer.position((int)(blocks.get(mid)-posFirst[mid/BLOCKS_PER_BYTEBUFFER]));
//			System.out.println("Comparing against block: "+ mid + " which is "+ ByteStringUtil.asString(buffer)+ " Result: "+cmp);

			if (cmp<0) {
				high = mid - 1;
			} else if (cmp > 0) {
				low = mid + 1;
			} else {
				return mid; // key found
			}
		}
		return -(low + 1);  // key not found.
	}
	
	
	/* (non-Javadoc)
	 * @see hdt.dictionary.DictionarySection#locate(java.lang.CharSequence)
	 */
	@Override
	public int locate(CharSequence str) {
		if(buffers==null || blocks==null) {
			return 0;
		}
		
		int blocknum = locateBlock(str);
		if(blocknum>=0) {
			// Located exactly
			return (blocknum*blocksize)+1;
		} else {
			// Not located exactly.
			blocknum = -blocknum-2;
			
			if(blocknum>=0) {
				int idblock = locateInBlock(blocknum, str);

				if(idblock != 0) {
					return (blocknum*blocksize)+idblock+1;
				}
			}
		}
		
		return 0;
	}
	
	public int locateInBlock(int block, CharSequence str) {
		if(block>=blocks.getNumberOfElements()) {
			return 0;
		}
		
		ReplazableString tempString = new ReplazableString();
		
		int idInBlock = 0;
		int cshared=0;
		
//		dumpBlock(block);
//		
//		System.out.println("Buff: "+ (block/BLOCKS_PER_BYTEBUFFER));
//		System.out.println("Off: "+ (block%BLOCKS_PER_BYTEBUFFER));
//		System.out.println("Block start: "+ blocks.get(block));
//		System.out.println("Buffer block start: "+ posFirst[block/BLOCKS_PER_BYTEBUFFER]);
		
		ByteBuffer buffer = cacheBuffers.get()[block/BLOCKS_PER_BYTEBUFFER];
		buffer.position((int)(blocks.get(block)-posFirst[block/BLOCKS_PER_BYTEBUFFER]));
		
		// Read the first string in the block
		try {
			if(!buffer.hasRemaining()) {
				return 0;
			}
			
			tempString.replace(buffer, 0);

			idInBlock++;

			while( (idInBlock<blocksize) && buffer.hasRemaining()) 
			{
				// Decode prefix
				long delta = VByte.decode(buffer);

				//Copy suffix
				tempString.replace(buffer, (int) delta);

				if(delta>=cshared)
				{
					// Current delta value means that this string
					// has a larger long common prefix than the previous one
					cshared += ByteStringUtil.longestCommonPrefix(tempString, str, cshared);

					if((cshared==str.length()) && (tempString.length()==str.length())) {
						break;
					}
				} else {
					// We have less common characters than before, 
					// this string is bigger that what we are looking for.
					// i.e. Not found.
					idInBlock = 0;
					break;
				}
				idInBlock++;
			}

			if(!buffer.hasRemaining() || idInBlock== blocksize) {
				idInBlock=0;
			}

			return idInBlock;
		} catch (IOException e) {
			e.printStackTrace();
			return 0;
		}
	}
	
	/* (non-Javadoc)
	 * @see hdt.dictionary.DictionarySection#extract(int)
	 */
	@Override
	public CharSequence extract(int id) {
		if(buffers==null || blocks==null) {
			return null;
		}
		
		if(id<1 || id>numstrings) {
			return null;
		}
		
		int block = (id-1)/blocksize;
		ByteBuffer buffer = cacheBuffers.get()[block/BLOCKS_PER_BYTEBUFFER];
		
		long blockPos = blocks.get(block);
		long blockBase = blocks.get(block-block%BLOCKS_PER_BYTEBUFFER);
		
		int pos = (int)(blockPos-blockBase);
		
//		System.out.println("Block: "+block);
//		System.out.println("Buffer: "+(block/BLOCKS_PER_BYTEBUFFER));
//		System.out.println("Remainder: "+(block%BLOCKS_PER_BYTEBUFFER));
//		System.out.println("Difference: "+(block-block%BLOCKS_PER_BYTEBUFFER));
//		
//		System.out.println("Block pos: "+blockPos);
//		System.out.println("Block base: "+blockBase);
//		System.out.println("Pos: "+pos);
		
		buffer.position(pos);
		
		try {
			ReplazableString tempString = new ReplazableString();
			tempString.replace(buffer,0);

			int stringid = (id-1)%blocksize;
			for(int i=0;i<stringid;i++) {
				long delta = VByte.decode(buffer);
				tempString.replace(buffer, (int) delta);
			}
			return tempString;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	/* (non-Javadoc)
	 * @see hdt.dictionary.DictionarySection#size()
	 */
	@Override
	public long size() {
		return dataSize+blocks.size();
	}

	/* (non-Javadoc)
	 * @see hdt.dictionary.DictionarySection#getNumberOfElements()
	 */
	@Override
	public int getNumberOfElements() {
		return numstrings;
	}

	/* (non-Javadoc)
	 * @see hdt.dictionary.DictionarySection#getEntries()
	 */
	@Override
	public Iterator<CharSequence> getSortedEntries() {
		return new Iterator<CharSequence>() {
			int id = 0;

			ReplazableString tempString = new ReplazableString();
			int bytebufferIndex=0;
			ByteBuffer buffer = cacheBuffers.get()[0];

			@Override
			public boolean hasNext() {
				return id<getNumberOfElements();
			}

			@Override
			public CharSequence next() {
				if(!buffer.hasRemaining()) {
					buffer = cacheBuffers.get()[++bytebufferIndex];
					buffer.rewind();
				}
				try {
					if((id%blocksize)==0) {
						tempString.replace(buffer, 0);
					} else {				
						long delta = VByte.decode(buffer);
						tempString.replace(buffer, (int) delta);
					}
					id++;
					return tempString.toString();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}

			@Override
			public void remove() {
				throw new NotImplementedException();
			}
		};
	}

	@Override
	public void close() throws IOException {
		ch.close();
	}

	@Override
	public void load(TempDictionarySection other, ProgressListener listener) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void save(OutputStream output, ProgressListener listener)
			throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void load(InputStream input, ProgressListener listener)
			throws IOException {
		// TODO Auto-generated method stub
		
	}
}