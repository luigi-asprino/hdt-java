/**
 * File: $HeadURL: https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/iterator/DictionaryTranslateIterator.java $
 * Revision: $Rev: 191 $
 * Last modified: $Date: 2013-03-03 11:41:43 +0000 (dom, 03 mar 2013) $
 * Last modified by: $Author: mario.arias $
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation;
 * version 3.0 of the License.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 *
 * Contacting the authors:
 *   Mario Arias:               mario.arias@deri.org
 *   Javier D. Fernandez:       jfergar@infor.uva.es
 *   Miguel A. Martinez-Prieto: migumar2@infor.uva.es
 */

package org.rdfhdt.hdt.iterator;

import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.rdfhdt.hdt.dictionary.impl.DictionaryPFCOptimizedExtractor;
import org.rdfhdt.hdt.dictionary.impl.FourSectionDictionary;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdt.triples.Triples;

/**
 * Iterator of TripleStrings based on IteratorTripleID
 * 
 */
public class DictionaryTranslateSpliteratorOptCanGoTo implements Spliterator<TripleString> {

	private final int blockSize;
	private Triples triples;
	private IteratorTripleID iterator;
	private DictionaryPFCOptimizedExtractor dictionary;
	private FourSectionDictionary originDictionary;
	private final CharSequence s, p, o;
	private final TripleID searchedTriple;
	private final long from;
	private AtomicLong currentIndex;
	private long toExcluded, estimatedSize;
	private Block current;

	public DictionaryTranslateSpliteratorOptCanGoTo(final Triples triples, final TripleID searchedTriple,
			final FourSectionDictionary dictionary, final CharSequence s, final CharSequence p, final CharSequence o,
			final int blockSize, final long from, final long toExcluded) {

		this.blockSize = blockSize;
		this.triples = triples;
		this.searchedTriple = searchedTriple;
		this.iterator = triples.search(searchedTriple);
		this.originDictionary = dictionary;
		this.toExcluded = toExcluded;
		this.from = from;
		this.dictionary = new DictionaryPFCOptimizedExtractor(dictionary);

		this.s = s == null ? "" : s;
		this.p = p == null ? "" : p;
		this.o = o == null ? "" : o;

		this.iterator.goTo(from);
		this.currentIndex = new AtomicLong(from);

		this.estimatedSize = toExcluded - from;

	}

	@Override
	public boolean tryAdvance(Consumer<? super TripleString> action) {
		if (current != null && current.hasNext()) {
			action.accept(current.next());
			return true;
		}

		if (currentIndex.longValue() < toExcluded && (current == null || !current.hasNext())) {
			BlockTripleID b = BlockTripleID.fetchBlock(iterator, currentIndex.longValue(),
					Math.min(currentIndex.longValue() + blockSize, toExcluded), this.dictionary, blockSize, s, p, o);
			if (current != null)
				current.destroy();
			current = Block.transformBlock(b, dictionary, blockSize, s, p, o);
			currentIndex.set(currentIndex.get() + b.getCount());
			b.destroy();
			b = null;
			if (current != null && current.hasNext()) {
				action.accept(current.next());
				return true;
			}
		}

		return false;
	}

	@Override
	public Spliterator<TripleString> trySplit() {
		if (iterator.hasNext() && (from + estimatedSize - currentIndex.longValue()) > blockSize * 2) {

			long splitted = ((from + estimatedSize - currentIndex.longValue()) / 2);
			long toExcludedNewIterator = this.toExcluded;
			long fromNewIterator = currentIndex.longValue() + splitted;
			this.toExcluded = fromNewIterator;
			this.estimatedSize = splitted;

			return new DictionaryTranslateSpliteratorOptCanGoTo(triples, searchedTriple, originDictionary, s, p, o,
					blockSize, fromNewIterator, toExcludedNewIterator);
		}
		return null;
	}

	@Override
	public int characteristics() {
		return Spliterator.DISTINCT | Spliterator.NONNULL | Spliterator.SIZED | Spliterator.SUBSIZED;
	}

	@Override
	public long estimateSize() {
		return estimatedSize;
	}

}