package org.rdfhdt.hdt.dictionary.impl;

import org.rdfhdt.hdt.dictionary.DictionaryIDMapping;
import org.rdfhdt.hdt.dictionary.impl.section.RocksTempDictionarySection;
import org.rocksdb.RocksDBException;

import it.cnr.istc.stlab.rocksmap.RocksMap;
import it.cnr.istc.stlab.rocksmap.transformer.LongRocksTransformer;

public class RocksDictionaryIDMapping implements DictionaryIDMapping {

	class Entry {
		long newid;
		final CharSequence str;

		Entry(CharSequence str) {
			this.str = str;
			this.newid = 0;
		}
	}

	private RocksMap<Long, Long> remapping;
	private RocksTempDictionarySection dictionarySection;
	private long lastId = 0;

	public RocksDictionaryIDMapping(String folder, RocksTempDictionarySection dictionarySection)
			throws RocksDBException {
//		list = new ArrayList<>((int) numentries);
		remapping = new RocksMap<>(folder, new LongRocksTransformer(), new LongRocksTransformer());
		this.dictionarySection = dictionarySection;
	}

	public void add(CharSequence str) {
//		list.add(new Entry(str));
		Long oldId = dictionarySection.locate(str);
		Long newId = lastId++;
		remapping.put(oldId, newId);
	}

	public void setNewID(long oldId, long newID) {
		remapping.put(oldId, newID);
	}

	public long getNewID(long oldId) {
		return remapping.get(oldId);
	}

	public CharSequence getString(long oldId) {
		return dictionarySection.extract(oldId);
	}

	public long size() {
		return remapping.sizeLong();
	}

}
