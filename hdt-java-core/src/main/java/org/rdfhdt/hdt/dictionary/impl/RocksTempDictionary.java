package org.rdfhdt.hdt.dictionary.impl;

import java.util.Iterator;

import org.rdfhdt.hdt.dictionary.DictionaryIDMapping;
import org.rdfhdt.hdt.dictionary.TempDictionarySection;
import org.rdfhdt.hdt.dictionary.impl.HashDictionary;
import org.rdfhdt.hdt.dictionary.impl.section.RocksTempDictionarySection;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.options.HDTOptions;
import org.rdfhdt.hdt.triples.TempTriples;
import org.rdfhdt.hdt.util.StopWatch;
import org.rocksdb.RocksDBException;

public class RocksTempDictionary extends HashDictionary {

	private String tempFolder;

	public RocksTempDictionary(HDTOptions spec) throws RocksDBException {
		super(spec);

		tempFolder = spec.get("tempfolder");

		// FIXME: Read types from spec
		subjects = new RocksTempDictionarySection(tempFolder + "/subjects");
		predicates = new RocksTempDictionarySection(tempFolder + "/predicates");
		objects = new RocksTempDictionarySection(tempFolder + "/objects");
		shared = new RocksTempDictionarySection(tempFolder + "/shared");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see hdt.dictionary.Dictionary#reorganize(hdt.triples.TempTriples)
	 */
	@Override
	public void reorganize(TempTriples triples) {
		try {
			DictionaryIDMapping mapSubj = new RocksDictionaryIDMapping(tempFolder + "/subjRemapping",
					(RocksTempDictionarySection) subjects);
			DictionaryIDMapping mapPred = new RocksDictionaryIDMapping(tempFolder + "/predRemapping",
					(RocksTempDictionarySection) predicates);
			DictionaryIDMapping mapObj = new RocksDictionaryIDMapping(tempFolder + "/objRemapping",
					(RocksTempDictionarySection) objects);

			StopWatch st = new StopWatch();

			// Generate old subject mapping
			Iterator<? extends CharSequence> itSubj = ((TempDictionarySection) subjects).getEntries();
			while (itSubj.hasNext()) {
				CharSequence str = itSubj.next();
				mapSubj.add(str);

				// GENERATE SHARED at the same time
				if (str.length() > 0 && str.charAt(0) != '"' && objects.locate(str) != 0) {
					shared.add(str);
				}
			}

			// Generate old predicate mapping
			st.reset();
			Iterator<? extends CharSequence> itPred = ((TempDictionarySection) predicates).getEntries();
			while (itPred.hasNext()) {
				CharSequence str = itPred.next();
				mapPred.add(str);
			}

			// Generate old object mapping
			Iterator<? extends CharSequence> itObj = ((TempDictionarySection) objects).getEntries();
			while (itObj.hasNext()) {
				CharSequence str = itObj.next();
				mapObj.add(str);
			}

			// Remove shared from subjects and objects
			Iterator<? extends CharSequence> itShared = ((TempDictionarySection) shared).getEntries();
			while (itShared.hasNext()) {
				CharSequence sharedStr = itShared.next();
				subjects.remove(sharedStr);
				objects.remove(sharedStr);
			}

			// Sort sections individually
			st.reset();
			subjects.sort();
			predicates.sort();
			objects.sort();
			shared.sort();

			// Update mappings with new IDs
			st.reset();
			for (long j = 0; j < mapSubj.size(); j++) {
				mapSubj.setNewID(j, this.stringToId(mapSubj.getString(j), TripleComponentRole.SUBJECT));
			}

			for (long j = 0; j < mapPred.size(); j++) {
				mapPred.setNewID(j, this.stringToId(mapPred.getString(j), TripleComponentRole.PREDICATE));
			}

			for (long j = 0; j < mapObj.size(); j++) {
				mapObj.setNewID(j, this.stringToId(mapObj.getString(j), TripleComponentRole.OBJECT));
			}
			// Replace old IDs with news
			triples.replaceAllIds(mapSubj, mapPred, mapObj);

			isOrganized = true;
		} catch (RocksDBException e) {
			e.printStackTrace();
		}
	}

}
