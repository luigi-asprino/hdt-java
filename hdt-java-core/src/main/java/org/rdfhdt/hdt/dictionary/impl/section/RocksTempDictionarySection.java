package org.rdfhdt.hdt.dictionary.impl.section;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
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
	private RocksBigList<CharSequence> list;
	private AtomicLong size = new AtomicLong(0);

	private Logger logger = LogManager.getLogger(RocksTempDictionarySection.class);

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
		list = new RocksBigList<>(dir + "/entryList", new CharSequenceRocksTransformer());
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
		return list.get(pos - 1);
	}

	@Override
	public long size() {
		return size.longValue();
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
	public synchronized long add(CharSequence entry) {
		CompactString compact = new CompactString(entry);
		Long pos = map.get(compact);
		if (pos != null) {
			return pos;
		}
		// Not found, insert new
		list.add(compact);
		map.put(compact, list.size64());
		size.addAndGet(compact.length());
		sorted = false;
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
		logger.info("Section List Cleant");
//		map.iterator().forEachRemaining(e -> list.add(e.getKey()));
		map.keyIterator().forEachRemaining(e -> list.add(e));
		logger.info("Section List Updated");

		list.sort(new CharSequenceComparator());
		logger.info("Section List Sorted");

//		for (long i = 0; i < list.size64(); i++) {
//			map.put(new CompactString(list.get(i)), i + 1);
//		}

		LongStream.range(0, list.size64()).parallel().forEach(i -> {
			map.put(new CompactString(list.get(i)), i + 1);
		});
		logger.info("Section Map Updated");

		sorted = true;

	}

	@Override
	public boolean isSorted() {
		return sorted;
	}

	@Override
	public void clear() {
		map.clear();
		list.clear();
		size = new AtomicLong(0);
		list = null; // because if sorted won't be anymore
	}

	@Override
	public void close() throws IOException {
		map.close();
		map = null;
		list.close();
		list = null;
	}

}
