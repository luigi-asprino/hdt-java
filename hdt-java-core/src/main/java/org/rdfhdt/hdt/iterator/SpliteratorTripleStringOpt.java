package org.rdfhdt.hdt.iterator;

import java.util.Spliterator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;

import com.google.common.collect.Queues;

public class SpliteratorTripleStringOpt implements Spliterator<TripleString> {
	private long estimatedSize;
	private IteratorTripleString baseIterator;
	private LinkedBlockingQueue<TripleString> buffer;
	private long smallestSegment = 10000;
	private final static int BUFFER_SIZE = 10000;

	public SpliteratorTripleStringOpt(IteratorTripleString baseIterator, long estimatedSize) {
		System.out.println("a");
		this.estimatedSize = estimatedSize;
		this.baseIterator = baseIterator;
		buffer = Queues.newLinkedBlockingQueue(BUFFER_SIZE);
		new Thread(() -> {
			while (baseIterator.hasNext()) {
				if (buffer.size() >= BUFFER_SIZE * 0.9) {
					try {
						Thread.sleep(10);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				buffer.add(baseIterator.next());
			}
		}).start();
	}

	public SpliteratorTripleStringOpt(IteratorTripleString baseIterator, LinkedBlockingQueue<TripleString> buffer,
			long estimatedSize) {
		this.estimatedSize = estimatedSize;
		this.buffer = buffer;
		this.baseIterator = baseIterator;
	}

	@Override
	public Spliterator<TripleString> trySplit() {
		if (baseIterator.hasNext() && estimatedSize > smallestSegment) {
			long splitted = (estimatedSize / 2);
			return new SpliteratorTripleStringOpt(baseIterator, buffer, splitted);
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
		TripleString elem;

		while (baseIterator.hasNext() && !buffer.isEmpty()) {
			try {
				elem = buffer.poll(100, TimeUnit.MILLISECONDS);
				if (elem != null) {
					action.accept(elem);
					return true;
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
				break;
			}
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
