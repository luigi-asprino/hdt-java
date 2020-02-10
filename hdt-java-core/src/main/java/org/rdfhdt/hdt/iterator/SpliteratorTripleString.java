package org.rdfhdt.hdt.iterator;

import java.util.Spliterator;
import java.util.function.Consumer;

import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;

public class SpliteratorTripleString implements Spliterator<TripleString> {
	private IteratorTripleString baseIterator;
	private long estimatedSize;
	private long smallestSegment = 10000;

	public SpliteratorTripleString(IteratorTripleString baseIterator, long estimatedSize) {
		this.baseIterator = baseIterator;
		this.estimatedSize = estimatedSize;
	}

	@Override
	public Spliterator<TripleString> trySplit() {
		if (estimatedSize > smallestSegment) {
			return new SpliteratorTripleString(baseIterator, estimatedSize / 2);
		}

		return null;
	}

	@Override
	public long estimateSize() {
		return estimatedSize;
	}

	@Override
	public int characteristics() {
		return 0;
	}

	@Override
	public boolean tryAdvance(Consumer<? super TripleString> action) {
		TripleString elem = null;
		synchronized (baseIterator) {
			if (baseIterator.hasNext()) {
				elem = baseIterator.next();
			}
		}
		if (elem != null) {
			action.accept(elem);
			return true;
		}
		return false;
	}

	public long getSmallestSegment() {
		return smallestSegment;
	}

	public void setSmallestSegment(long smallestSegment) {
		this.smallestSegment = smallestSegment;
	}

}
