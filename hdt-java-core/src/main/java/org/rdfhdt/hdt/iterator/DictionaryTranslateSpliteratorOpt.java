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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.rdfhdt.hdt.dictionary.impl.DictionaryPFCOptimizedExtractor;
import org.rdfhdt.hdt.dictionary.impl.FourSectionDictionary;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleString;

import com.google.common.collect.Queues;

/**
 * Iterator of TripleStrings based on IteratorTripleID
 * 
 */
public class DictionaryTranslateSpliteratorOpt implements Spliterator<TripleString> {
	private static final int DEFAULT_BLOCK_SIZE = 10000;
	private static final int NUMBER_OF_BUFFERED_BLOCKS = Runtime.getRuntime().availableProcessors() * 2;
	private final int blockSize;
	private IteratorTripleID iterator;
	private DictionaryPFCOptimizedExtractor dictionary;
	private FourSectionDictionary originDictionary;
	private CharSequence s, p, o;
	private Block current;
	private BlockingQueue<BlockTripleID> nextBlocks;
	private long estimatedSize;

	public DictionaryTranslateSpliteratorOpt(IteratorTripleID iteratorTripleID, FourSectionDictionary dictionary,
			CharSequence s, CharSequence p, CharSequence o, int blockSize) {

		System.out.println("Spliterator opt " + blockSize+ " "+iteratorTripleID.canGoTo()+" "+iteratorTripleID.getClass().toString());

		this.blockSize = blockSize;
		this.iterator = iteratorTripleID;
		this.originDictionary = dictionary;
		this.dictionary = new DictionaryPFCOptimizedExtractor(dictionary);

		this.s = s == null ? "" : s;
		this.p = p == null ? "" : p;
		this.o = o == null ? "" : o;

		this.estimatedSize = iteratorTripleID.estimatedNumResults();
		nextBlocks = Queues.newLinkedBlockingQueue(NUMBER_OF_BUFFERED_BLOCKS);
		new Thread(() -> fetchNextBlocks()).start();
	}

	private DictionaryTranslateSpliteratorOpt(IteratorTripleID iteratorTripleID, FourSectionDictionary dictionary,
			BlockingQueue<BlockTripleID> nextBlocks, CharSequence s, CharSequence p, CharSequence o, int blockSize,
			long estimatedSize) {
		

		this.blockSize = blockSize;
		this.iterator = iteratorTripleID;
		this.originDictionary = dictionary;
		this.dictionary = new DictionaryPFCOptimizedExtractor(dictionary);
		
		this.s = s == null ? "" : s;
		this.p = p == null ? "" : p;
		this.o = o == null ? "" : o;

		this.estimatedSize = estimatedSize;
		this.nextBlocks = nextBlocks;

	}

	private void fetchNextBlocks() {
		while (iterator.hasNext()) {
			try {
				nextBlocks.put(BlockTripleID.fetchBlock(iterator, this.dictionary, blockSize, s, p, o));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public boolean tryAdvance(Consumer<? super TripleString> action) {
		if (current != null && current.hasNext()) {
			action.accept(current.next());
			return true;
		}

		if (current == null && !nextBlocks.isEmpty()) {
			BlockTripleID currentBlockTripleID = nextBlocks.poll();
			if (currentBlockTripleID != null) {
				current = Block.transformBlock(currentBlockTripleID, dictionary, blockSize, s, p, o);
				if (current != null && current.hasNext()) {
					action.accept(current.next());
					return true;
				}
			}

		}

		if (iterator.hasNext()) {
			try {
				BlockTripleID currentBlockTripleID = nextBlocks.poll(10, TimeUnit.MILLISECONDS);
				if (currentBlockTripleID != null) {
					current = Block.transformBlock(currentBlockTripleID, dictionary, blockSize, s, p, o);
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		if (current != null && current.hasNext()) {
			action.accept(current.next());
			return true;
		}

		return false;
	}

	@Override
	public Spliterator<TripleString> trySplit() {
		if (iterator.hasNext() && estimatedSize > DEFAULT_BLOCK_SIZE * 2) {
			long splitted = (estimatedSize / 2);
			return new DictionaryTranslateSpliteratorOpt(iterator, originDictionary, nextBlocks, s, p, o, blockSize,
					splitted);
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