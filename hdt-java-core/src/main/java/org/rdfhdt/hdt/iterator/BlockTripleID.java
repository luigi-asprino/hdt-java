package org.rdfhdt.hdt.iterator;

import java.util.ArrayList;
import java.util.List;

import org.rdfhdt.hdt.dictionary.impl.DictionaryPFCOptimizedExtractor;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;

public final class BlockTripleID {

	private long[] subject, predicate, object;

	final private List<TripleID> triples;

	private int count = 0;

	final private CharSequence s, p, o;

	BlockTripleID(final int blockSize, final CharSequence s, final CharSequence p, final CharSequence o) {

		this.s = s == null ? "" : s;
		this.p = p == null ? "" : p;
		this.o = o == null ? "" : o;

		triples = new ArrayList<>(blockSize);

		if (this.s.length() == 0) {
			this.subject = new long[blockSize];
		}

		if (this.p.length() == 0) {
			this.predicate = new long[blockSize];
		}

		if (this.o.length() == 0) {
			this.object = new long[blockSize];
		}

	}

	static BlockTripleID fetchBlock(final IteratorTripleID iterator, final DictionaryPFCOptimizedExtractor dictionary,
			final int blockSize, final CharSequence s, final CharSequence p, final CharSequence o) {

		final BlockTripleID r = new BlockTripleID(blockSize, s, p, o);

		for (int i = 0; i < blockSize && iterator.hasNext(); i++) {
			r.count++;
			final TripleID t = iterator.next();
			r.triples.add(t);
			if (s.length() == 0) {
				r.subject[i] = t.getSubject();
			}

			if (p.length() == 0) {
				r.predicate[i] = t.getPredicate();
			}

			if (o.length() == 0) {
				r.object[i] = t.getObject();
			}
		}

		return r;
	}

	public long[] getSubject() {
		return subject;
	}

	public long[] getPredicate() {
		return predicate;
	}

	public long[] getObject() {
		return object;
	}

	public int getCount() {
		return count;
	}

	public List<TripleID> getTriples() {
		return triples;
	}

}
