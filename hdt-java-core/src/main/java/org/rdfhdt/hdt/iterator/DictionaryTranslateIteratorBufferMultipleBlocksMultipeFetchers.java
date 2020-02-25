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

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.rdfhdt.hdt.dictionary.impl.DictionaryPFCOptimizedExtractor;
import org.rdfhdt.hdt.dictionary.impl.FourSectionDictionary;
import org.rdfhdt.hdt.enums.ResultEstimationType;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;

/**
 * Iterator of TripleStrings based on IteratorTripleID
 * 
 */
public class DictionaryTranslateIteratorBufferMultipleBlocksMultipeFetchers implements IteratorTripleString {
	private static final int DEFAULT_BLOCK_SIZE = 10000;

	private final int blockSize;

	private IteratorTripleID iterator;
	private DictionaryPFCOptimizedExtractor dictionary;
	private CharSequence s, p, o;

	private Block current;

	private ConcurrentLinkedQueue<Block> nextBlocks;

	private AtomicBoolean toFetch = new AtomicBoolean(true);

	public DictionaryTranslateIteratorBufferMultipleBlocksMultipeFetchers(IteratorTripleID iteratorTripleID,
			FourSectionDictionary dictionary, CharSequence s, CharSequence p, CharSequence o) {
		this(iteratorTripleID, dictionary, s, p, o, DEFAULT_BLOCK_SIZE);
	}

	public DictionaryTranslateIteratorBufferMultipleBlocksMultipeFetchers(IteratorTripleID iteratorTripleID,
			FourSectionDictionary dictionary, CharSequence s, CharSequence p, CharSequence o, int blockSize) {

		System.out.println("Buffer blocks");
		this.blockSize = blockSize;
		this.iterator = iteratorTripleID;
		this.dictionary = new DictionaryPFCOptimizedExtractor(dictionary);

		nextBlocks = new ConcurrentLinkedQueue<>();

		this.s = s == null ? "" : s;
		this.p = p == null ? "" : p;
		this.o = o == null ? "" : o;

		// fetching first block
		current = Block.fetchBlock(iterator, this.dictionary, blockSize, s, p, o);

		new Thread(() -> fetchNextBlocks()).start();
	}

	private void fetchBlock() {
		while (nextBlocks.isEmpty() && iterator.hasNext()) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		current = nextBlocks.poll();
	}

	private void fetchNextBlocks() {
		while (iterator.hasNext()) {
			nextBlocks.add(Block.fetchBlock(iterator, this.dictionary, blockSize, s, p, o));
		}
	}

	@Override
	public boolean hasNext() {

		if (current != null && !current.hasNext()) {
			fetchBlock();
		}

		if (current == null) {
			return false;
		}

		return current != null && current.hasNext();
	}

	@Override
	public TripleString next() {
		return current.next();
	}

	@Override
	public void remove() {
		iterator.remove();
	}

	@Override
	public void goToStart() {
		throw new UnsupportedOperationException();
	}

	@Override
	public long estimatedNumResults() {
		return iterator.estimatedNumResults();
	}

	@Override
	public ResultEstimationType numResultEstimation() {
		return iterator.numResultEstimation();
	}

}