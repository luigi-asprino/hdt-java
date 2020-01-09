package org.rdfhdt.hdt.dictionary.impl;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

import org.rdfhdt.hdt.dictionary.DictionaryIDMapping;
import org.rocksdb.RocksDBException;

import it.cnr.istc.stlab.rocksmap.RocksBigList;
import it.cnr.istc.stlab.rocksmap.transformer.RocksTransformer;

public class RocksDictionaryIDMapping implements DictionaryIDMapping {

	class Entry {
		long newid;
		final CharSequence str;

		Entry(CharSequence str) {
			this.str = str;
			this.newid = 0;
		}
	}

	class RocksEntryTransformer implements RocksTransformer<Entry> {

		public byte[] transform(Entry value) {
			byte[] strBytes = value.str.toString().getBytes();
			ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
			buffer.putLong(value.newid);
			byte[] longBytes = buffer.array();
			byte[] result = new byte[strBytes.length + longBytes.length];
			System.arraycopy(longBytes, 0, result, 0, longBytes.length);
			System.arraycopy(strBytes, 0, result, longBytes.length, strBytes.length);
			return result;
		}

		@Override
		public Entry transform(byte[] value) {
			ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
			byte[] longBytes = new byte[Long.BYTES];
			buffer.put(longBytes);
			buffer.flip();
			Long newid = buffer.getLong();
			byte[] strBytes = Arrays.copyOfRange(value, longBytes.length, value.length);
			Entry e = new Entry(new String(strBytes));
			e.newid = newid;

			return e;
		}

		@Override
		public Collection<Entry> transformCollection(byte[] value) {
			throw new UnsupportedOperationException();
		}
	}

	final RocksBigList<Entry> list;

	public RocksDictionaryIDMapping(String folder) throws RocksDBException {
		list = new RocksBigList<>(folder, new RocksEntryTransformer());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.rdfhdt.hdt.dictionary.impl.DictionaryIDMappin#add(java.lang.CharSequence)
	 */
	@Override
	public void add(CharSequence str) {
		list.add(new Entry(str));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.rdfhdt.hdt.dictionary.impl.DictionaryIDMappin#setNewID(long, long)
	 */
	@Override
	public void setNewID(long oldId, long newID) {
		list.get(oldId).newid = newID;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.rdfhdt.hdt.dictionary.impl.DictionaryIDMappin#getNewID(long)
	 */
	@Override
	public long getNewID(long oldId) {
		// System.out.println("GetNewID old: "+oldId+"/"+list.size());
		return list.get(oldId).newid;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.rdfhdt.hdt.dictionary.impl.DictionaryIDMappin#getString(long)
	 */
	@Override
	public CharSequence getString(long oldId) {
		return list.get(oldId).str;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.rdfhdt.hdt.dictionary.impl.DictionaryIDMappin#size()
	 */
	@Override
	public long size() {
		return list.size64();
	}

}
