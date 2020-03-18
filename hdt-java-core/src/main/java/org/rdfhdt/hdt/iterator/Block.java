package org.rdfhdt.hdt.iterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.rdfhdt.hdt.dictionary.impl.DictionaryPFCOptimizedExtractor;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdt.triples.TripleString;

import com.google.common.collect.Maps;

public final class Block implements Iterator<TripleString> {

	private Iterator<TripleID> iteratorTripleID;
	private Map<Long, CharSequence> mapSubject, mapPredicate, mapObject;

	private long lastSid, lastPid, lastOid;
	private CharSequence lastSstr, lastPstr, lastOstr;

	final private CharSequence s, p, o;

	Block(final int blockSize, final CharSequence s, final CharSequence p, final CharSequence o) {

		this.s = s == null ? "" : s;
		this.p = p == null ? "" : p;
		this.o = o == null ? "" : o;

		if (s.length() == 0) {
			mapSubject = Maps.newHashMapWithExpectedSize(blockSize);
		}

		if (p.length() == 0) {
			mapPredicate = Maps.newHashMapWithExpectedSize(blockSize);
		}

		if (o.length() == 0) {
			mapObject = Maps.newHashMapWithExpectedSize(blockSize);
		}
	}

	static void fill(DictionaryPFCOptimizedExtractor dictionary, long[] arr, final int count,
			Map<Long, CharSequence> map, TripleComponentRole role) {

		Arrays.sort(arr, 0, count);

		long last = -1;
		for (int i = 0; i < count; i++) {
			long val = arr[i];

			if (val != last) {
				CharSequence str = dictionary.idToString(val, role);

				map.put(val, str);

				last = val;
			}
		}

	}

	static Block fetchBlock(final IteratorTripleID iterator, DictionaryPFCOptimizedExtractor dictionary,
			final int blockSize, final CharSequence s, final CharSequence p, final CharSequence o) {
		List<TripleID> triples = new ArrayList<TripleID>(blockSize);
		Block r = new Block(blockSize, s, p, o);

		long[] arrSubjects = new long[blockSize];
		long[] arrPredicates = new long[blockSize];
		long[] arrObjects = new long[blockSize];

		int count = 0;
		for (int i = 0; i < blockSize && iterator.hasNext(); i++) {
			TripleID t = new TripleID(iterator.next());

			triples.add(t);

			if (s.length() == 0) {
				arrSubjects[count] = t.getSubject();
			}
			if (p.length() == 0) {
				arrPredicates[count] = t.getPredicate();
			}
			if (o.length() == 0)
				arrObjects[count] = t.getObject();

			count++;

		}
		ExecutorService pool = Executors.newFixedThreadPool(3);
		final int c = count;
		if (s.length() == 0) {
			pool.execute(() -> {
				fill(dictionary, arrSubjects, c, r.mapSubject, TripleComponentRole.SUBJECT);
			});
		}
		if (p.length() == 0) {
			pool.execute(() -> {
				fill(dictionary, arrPredicates, c, r.mapPredicate, TripleComponentRole.PREDICATE);
			});
		}
		if (o.length() == 0) {
			pool.execute(() -> {
				fill(dictionary, arrObjects, c, r.mapObject, TripleComponentRole.OBJECT);
			});
		}

		try {
			pool.shutdown();
			pool.awaitTermination(10, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		r.iteratorTripleID = triples.iterator();

		return r;
	}

	static Block transformBlock(BlockTripleID blockOriginal, DictionaryPFCOptimizedExtractor dictionary,
			final int blockSize, final CharSequence s, final CharSequence p, final CharSequence o) {
		Block r = new Block(blockSize, s, p, o);

		ExecutorService pool = Executors.newFixedThreadPool(3);
		final int c = blockOriginal.getCount();

		if (s.length() == 0) {

			pool.execute(() -> {
				fill(dictionary, blockOriginal.getSubject(), c, r.mapSubject, TripleComponentRole.SUBJECT);
			});
		}
		if (p.length() == 0) {

			pool.execute(() -> {
				fill(dictionary, blockOriginal.getPredicate(), c, r.mapPredicate, TripleComponentRole.PREDICATE);
			});
		}
		if (o.length() == 0) {

			pool.execute(() -> {
				fill(dictionary, blockOriginal.getObject(), c, r.mapObject, TripleComponentRole.OBJECT);
			});
		}

		try {
			pool.shutdown();
			pool.awaitTermination(10, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		r.iteratorTripleID = blockOriginal.getTriples().iterator();

		return r;
	}

	@Override
	public boolean hasNext() {
		return iteratorTripleID != null && iteratorTripleID.hasNext();
	}

	@Override
	public TripleString next() {
		TripleID triple = iteratorTripleID.next();

		if (s.length() != 0) {
			lastSstr = s;
		} else if (triple.getSubject() != lastSid) {
			lastSid = triple.getSubject();
			lastSstr = mapSubject.get(lastSid);
		}

		if (p.length() != 0) {
			lastPstr = p;
		} else if (triple.getPredicate() != lastPid) {
			lastPid = triple.getPredicate();
			lastPstr = mapPredicate.get(lastPid);
		}

		if (o.length() != 0) {
			lastOstr = o;
		} else if (triple.getObject() != lastOid) {
			lastOid = triple.getObject();
			lastOstr = mapObject.get(lastOid);
		}

		return new TripleString(lastSstr, lastPstr, lastOstr);
	}

	public void destroy() {
		mapSubject = mapPredicate = mapObject = null;
		iteratorTripleID = null;
	}

}
