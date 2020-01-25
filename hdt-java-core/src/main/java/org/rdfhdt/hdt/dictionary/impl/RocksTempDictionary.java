package org.rdfhdt.hdt.dictionary.impl;

import java.io.IOException;
import java.util.Iterator;
import java.util.stream.LongStream;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.rdfhdt.hdt.dictionary.TempDictionarySection;
import org.rdfhdt.hdt.dictionary.impl.section.RocksTempDictionarySection;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.options.HDTOptions;
import org.rdfhdt.hdt.triples.TempTriples;
import org.rdfhdt.hdt.util.StopWatch;
import org.rocksdb.RocksDBException;

public class RocksTempDictionary extends HashDictionary {

	private Logger logger = LogManager.getLogger(RocksTempDictionary.class);

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
		// TODO use fork/join pool instead of Thread
//		Executor e = Executors.newCachedThreadPool()

		logger.info("Reorganising Dictionary");

		try {
			RocksDictionaryIDMapping mapSubj = new RocksDictionaryIDMapping(tempFolder + "/subjRemapping");
			RocksDictionaryIDMapping mapPred = new RocksDictionaryIDMapping(tempFolder + "/predRemapping");
			RocksDictionaryIDMapping mapObj = new RocksDictionaryIDMapping(tempFolder + "/objRemapping");

			StopWatch st = new StopWatch();

			// Generate old subject mapping
			Runnable rSubj = () -> {
				Iterator<? extends CharSequence> itSubj = ((TempDictionarySection) subjects).getEntries();
				// TODO parallelize
				while (itSubj.hasNext()) {
					CharSequence str = itSubj.next();
					mapSubj.add(str);

					// GENERATE SHARED at the same time
					if (str.length() > 0 && str.charAt(0) != '"' && objects.locate(str) != 0) {
						shared.add(str);
					}
				}
				logger.info("Log ID old subject mapping generated!");
			};

			// Generate old predicate mapping
			st.reset();
			Runnable rPred = () -> {
				Iterator<? extends CharSequence> itPred = ((TempDictionarySection) predicates).getEntries();
				while (itPred.hasNext()) {
					// TODO parallelize
					CharSequence str = itPred.next();
					mapPred.add(str);
				}
				logger.info("Log ID old predicate mapping generated!");
			};

			// Generate old object mapping
			Runnable rObj = () -> {
				Iterator<? extends CharSequence> itObj = ((TempDictionarySection) objects).getEntries();
				while (itObj.hasNext()) {
					// TODO parallelize
					CharSequence str = itObj.next();
					mapObj.add(str);
				}
				logger.info("Log ID old object mapping generated!");
			};

			Thread tSubj = new Thread(rSubj);
			Thread tPred = new Thread(rPred);
			Thread tObj = new Thread(rObj);

			tSubj.start();
			tPred.start();
			tObj.start();

			tSubj.join();
			tPred.join();
			tObj.join();

			System.gc();

			// Remove shared from subjects and objects
			Iterator<? extends CharSequence> itShared = ((TempDictionarySection) shared).getEntries();
			while (itShared.hasNext()) {
				// TODO parallelize
				CharSequence sharedStr = itShared.next();
				subjects.remove(sharedStr);
				objects.remove(sharedStr);
			}

			// Sort sections individually
			st.reset();

			logger.info("Shared section updated!");

			Thread sortSubj = new Thread(() -> subjects.sort());
			Thread sortPred = new Thread(() -> predicates.sort());
			Thread sortObj = new Thread(() -> objects.sort());
			Thread sortSha = new Thread(() -> shared.sort());

			sortSubj.start();
			sortPred.start();
			sortObj.start();
			sortSha.start();

			sortSubj.join();
			sortPred.join();
			sortObj.join();
			sortSha.join();

			System.gc();

			logger.info("Sections sorted");

			// Update mappings with new IDs
			st.reset();
			Runnable updateIdSubj = () -> {
//				for (long j = 0; j < mapSubj.size(); j++) {
//					mapSubj.setNewID(j, this.stringToId(mapSubj.getString(j), TripleComponentRole.SUBJECT));
//				}
				LongStream.range(0, mapSubj.size()).parallel().forEach(j -> {
					mapSubj.setNewID(j, this.stringToId(mapSubj.getString(j), TripleComponentRole.SUBJECT));
				});
				logger.info("subject mapping updated!");
			};
			Runnable updateIdPred = () -> {
//				for (long j = 0; j < mapPred.size(); j++) {
//					mapPred.setNewID(j, this.stringToId(mapPred.getString(j), TripleComponentRole.PREDICATE));
//				}
				LongStream.range(0, mapPred.size()).parallel().forEach(j -> {
					mapPred.setNewID(j, this.stringToId(mapPred.getString(j), TripleComponentRole.PREDICATE));
				});
				logger.info("predicate mapping updated!");
			};
			Runnable updateIdObje = () -> {
//				for (long j = 0; j < mapObj.size(); j++) {
//					mapObj.setNewID(j, this.stringToId(mapObj.getString(j), TripleComponentRole.OBJECT));
//				}
				LongStream.range(0, mapObj.size()).parallel().forEach(j -> {
					mapObj.setNewID(j, this.stringToId(mapObj.getString(j), TripleComponentRole.OBJECT));
				});
				logger.info("object mapping updated!");
			};

			Thread uSubj = new Thread(updateIdSubj);
			Thread uPred = new Thread(updateIdPred);
			Thread uObj = new Thread(updateIdObje);

			uSubj.start();
			uPred.start();
			uObj.start();

			uSubj.join();
			uPred.join();
			uObj.join();

			System.gc();

			// Replace old IDs with news
			triples.replaceAllIds(mapSubj, mapPred, mapObj);

			mapSubj.list.clear();
			mapSubj.list.close();

			mapPred.list.clear();
			mapPred.list.close();

			mapObj.list.clear();
			mapObj.list.close();

			System.gc();

			logger.info("new ids replaced in triples (# " + triples.getNumberOfElements() + ")");

			// System.out.println("Replace IDs in "+st.stopAndShow());
			isOrganized = true;

		} catch (RocksDBException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void close() {
		try {
			subjects.close();
			predicates.close();
			objects.close();
			shared.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
