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
		this(baseIterator, estimatedSize, 100000);
	}

	public SpliteratorTripleString(IteratorTripleString baseIterator, long estimatedSize, long smallestSegment) {
		this.baseIterator = baseIterator;
		this.estimatedSize = estimatedSize;
		this.smallestSegment = smallestSegment;
	}

	@Override
	public Spliterator<TripleString> trySplit() {
		if (baseIterator.hasNext() && estimatedSize > smallestSegment) {
			long splitted = (estimatedSize / 2);
			
			return new SpliteratorTripleString(baseIterator, splitted);
		}
		return null;
	}

	@Override
	public long estimateSize() {
		return estimatedSize;
	}

	@Override
	public int characteristics() {
		return Spliterator.DISTINCT | Spliterator.NONNULL | Spliterator.SIZED | Spliterator.SUBSIZED;
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
