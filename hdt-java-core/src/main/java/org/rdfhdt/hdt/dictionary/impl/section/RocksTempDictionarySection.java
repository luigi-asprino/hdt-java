package org.rdfhdt.hdt.dictionary.impl.section;

import java.io.IOException;
import java.util.Iterator;

import org.rdfhdt.hdt.dictionary.TempDictionarySection;
import org.rdfhdt.hdt.options.HDTOptions;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.util.string.CharSequenceComparator;
import org.rdfhdt.hdt.util.string.CompactString;
import org.rocksdb.RocksDBException;

import it.cnr.istc.stlab.rocksmap.RocksBigList;
import it.cnr.istc.stlab.rocksmap.RocksMap;
import it.cnr.istc.stlab.rocksmap.transformer.LongRocksTransformer;

public class RocksTempDictionarySection implements TempDictionarySection {

	public static final int TYPE_INDEX = 2;

	private RocksMap<CharSequence, Long> map;
//	private RocksMap<Long, CompactString> inverse;
	private RocksBigList<CharSequence> list;
	private int size;
	boolean sorted = false;

	/**
	 * @throws RocksDBException
	 * 
	 */
	public RocksTempDictionarySection(String dir) throws RocksDBException {
		this(new HDTSpecification(), dir);
	}

	public RocksTempDictionarySection(HDTOptions spec, String dir) throws RocksDBException {
		map = new RocksMap<>(dir + "/direct", new CharSequenceRocksTransformer(), new LongRocksTransformer());
		list = new RocksBigList<CharSequence>(dir + "/entryList", new CharSequenceRocksTransformer());
		size = 0;
	}

	@Override
	public long locate(CharSequence s) {
		CompactString compact = new CompactString(s);
		Long val = map.get(compact);
		if (val == null) {
			return 0;
		}
		return val;
	}

	@Override
	public CharSequence extract(long pos) {

		if (pos <= 0) {
			return null;
		}
		return list.get((int) (pos - 1));

	}

	@Override
	public long size() {
		return size;
	}

	@Override
	public long getNumberOfElements() {
		return list.size64();
	}

	@Override
	public Iterator<? extends CharSequence> getSortedEntries() {
		if (list == null)
			return null;
		return list.iterator();
	}

	@Override
	public Iterator<? extends CharSequence> getEntries() {
		return list.iterator();
	}

	@Override
	public long add(CharSequence entry) {
		CompactString compact = new CompactString(entry);
		Long pos = map.get(compact);
		if (pos != null) {
			// Found return existing ID.
			return pos;
		}

		// Not found, insert new
		list.add(compact);
		map.put(compact, list.size64());

		size += compact.length();
		sorted = false;
//		list = null;

		return list.size64();
	}

	@Override
	public void remove(CharSequence seq) {
		map.remove(seq);
		sorted = false;
	}

	@Override
	public void sort() {
		// Update list.
		list.clear();
		map.iterator().forEachRemaining(e -> list.add(e.getKey()));

		list.sort(new CharSequenceComparator());

		for (long i = 0; i < list.size64(); i++) {
			map.put(new CompactString(list.get(i)), i + 1);
		}

	}

	@Override
	public boolean isSorted() {
		return sorted;
	}

	@Override
	public void clear() {
//		list.clear();
		map.clear();
		list.clear();
		size = 0;
		list = null; // because if sorted won't be anymore
	}

	@Override
	public void close() throws IOException {
		map.close();
		map = null;
		list.close();
		list = null;
		list = null;
	}

}
