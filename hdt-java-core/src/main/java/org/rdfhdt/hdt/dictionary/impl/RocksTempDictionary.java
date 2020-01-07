package org.rdfhdt.hdt.dictionary.impl;

import org.rdfhdt.hdt.dictionary.impl.HashDictionary;
import org.rdfhdt.hdt.dictionary.impl.section.RocksTempDictionarySection;
import org.rdfhdt.hdt.options.HDTOptions;
import org.rocksdb.RocksDBException;

public class RocksTempDictionary extends HashDictionary {

	public RocksTempDictionary(HDTOptions spec) throws RocksDBException {
		super(spec);

		String tempFolder = spec.get("tempfolder");

		// FIXME: Read types from spec
		subjects = new RocksTempDictionarySection(tempFolder + "/subjects");
		predicates = new RocksTempDictionarySection(tempFolder + "/predicates");
		objects = new RocksTempDictionarySection(tempFolder + "/objects");
		shared = new RocksTempDictionarySection(tempFolder + "/shared");
	}

}
