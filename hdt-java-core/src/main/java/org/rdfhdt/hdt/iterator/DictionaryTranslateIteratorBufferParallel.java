package org.rdfhdt.hdt.iterator;

import java.util.Spliterator;
import java.util.function.Consumer;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.rdfhdt.hdt.dictionary.impl.FourSectionDictionary;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdt.triples.TripleString;

public class DictionaryTranslateIteratorBufferParallel extends DictionaryTranslateIteratorBuffer
		implements Spliterator<TripleString> {

	long limit = -1, firstPosition;
	FourSectionDictionary oldDictionary;
	private Logger logger = LogManager.getLogger(DictionaryTranslateIteratorBufferParallel.class);

	public DictionaryTranslateIteratorBufferParallel(IteratorTripleID iteratorTripleID,
			FourSectionDictionary dictionary, CharSequence s, CharSequence p, CharSequence o, long firstPosition,
			long limit) {
		super(iteratorTripleID, dictionary, s, p, o);
		if (iteratorTripleID.canGoTo()) {
			logger.trace(String.format("new iterator first position %s %s", firstPosition, limit));
			iteratorTripleID.goTo(firstPosition);
		} else {
			iteratorTripleID.goToStart();
		}
		this.firstPosition = firstPosition;
		if (limit < 0) {
			this.limit = estimatedNumResults();
		} else {
			this.limit = limit;
		}
		this.oldDictionary = dictionary;
	}

	@Override
	public boolean hasNext() {
		boolean more = (child.hasNext() || iterator.hasNext()) && (firstPosition < limit);
		if (!more) {
			mapSubject = mapPredicate = mapObject = null;
			triples = null;
		}
		return more;
	}

	@Override
	public TripleString next() {
		if (!child.hasNext()) {
			fetchBlock();
		}

		TripleID triple = child.next();

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
		firstPosition++;
		return new TripleString(lastSstr, lastPstr, lastOstr);
	}

	@Override
	public boolean tryAdvance(Consumer<? super TripleString> action) {
		if (hasNext()) {
			action.accept(next());
			return true;
		}
		return false;
	}

	@Override
	public Spliterator<TripleString> trySplit() {
		if (super.iterator.canGoTo()) {
			long remaining = limit - firstPosition;
			if (remaining <= blockSize)
				return null;
			final long oldLimit = limit;
			this.limit = firstPosition + (remaining / 2);
			return new DictionaryTranslateIteratorBufferParallel(iterator.clone(), oldDictionary, s, p, o, this.limit,
					oldLimit);
		}
		return null;
	}

	@Override
	public long estimateSize() {
		return limit - firstPosition;
	}

	@Override
	public int characteristics() {
		return ORDERED | SIZED | IMMUTABLE | SUBSIZED;
	}

	@Override
	public void forEachRemaining(Consumer<? super TripleString> action) {
		Spliterator.super.forEachRemaining(action);
	}

}
