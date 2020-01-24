package org.rdfhdt.hdt.triples.impl;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.rdfhdt.hdt.dictionary.DictionaryIDMapping;
import org.rdfhdt.hdt.enums.ResultEstimationType;
import org.rdfhdt.hdt.enums.TripleComponentOrder;
import org.rdfhdt.hdt.exceptions.NotImplementedException;
import org.rdfhdt.hdt.hdt.HDTVocabulary;
import org.rdfhdt.hdt.header.Header;
import org.rdfhdt.hdt.iterator.SequentialSearchIteratorTripleID;
import org.rdfhdt.hdt.listener.ProgressListener;
import org.rdfhdt.hdt.options.ControlInfo;
import org.rdfhdt.hdt.options.HDTOptions;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TempTriples;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdt.triples.TripleIDComparator;
import org.rdfhdt.hdt.triples.Triples;
import org.rdfhdt.hdt.util.RDFInfo;
import org.rdfhdt.hdt.util.io.CountInputStream;
import org.rdfhdt.hdt.util.io.IOUtil;
import org.rdfhdt.hdt.util.listener.ListenerUtil;
import org.rocksdb.RocksDBException;

import it.cnr.istc.stlab.rocksmap.RocksBigList;

/**
 * Implementation of TempTriples using a List of TripleID.
 * 
 */
public class RocksTriples implements TempTriples {

	/** The array to hold the triples */
	private RocksBigList<TripleID> arrayOfTriples;

	private static final Logger logger = LogManager.getLogger(RocksTriples.class);

	/** The order of the triples */
	private TripleComponentOrder order;
	private AtomicLong numValidTriples;

	private boolean sorted = false;

	/**
	 * Constructor, given an order to sort by
	 * 
	 * @param order The order to sort by
	 * @throws RocksDBException
	 */
	public RocksTriples(HDTOptions specification) throws RocksDBException {

		String tempFolder = specification.get("tempfolder");

		// precise allocation of the array (minimal memory wasting)
		long numTriples = RDFInfo.getTriples(specification);
		numTriples = (numTriples > 0) ? numTriples : 100;
		this.arrayOfTriples = new RocksBigList<TripleID>(tempFolder + "/triples", new TripleIDTransformer());

		// choosing starting(or default) component order
		String orderStr = specification.get("triplesOrder");

		if (orderStr == null) {
			orderStr = "SPO";
		}
		this.order = TripleComponentOrder.valueOf(orderStr);
		

		this.numValidTriples = new AtomicLong(arrayOfTriples.sizeLong());
	}

	/**
	 * A method for setting the size of the arrayList (so no reallocation occurs).
	 * If not empty does nothing and returns false.
	 */
	// TODO
//	public boolean reallocateIfEmpty(int numTriples) {
//		if (arrayOfTriples.isEmpty()) {
//			arrayOfTriples = new ArrayList<TripleID>(numTriples);
//			return true;
//		} else {
//			return false;
//		}
//	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see hdt.triples.Triples#search(hdt.triples.TripleID)
	 */
	@Override
	public IteratorTripleID search(TripleID pattern) {
		String patternStr = pattern.getPatternString();
		if (patternStr.equals("???")) {
			return new TriplesListIterator(this);
		} else {
			return new SequentialSearchIteratorTripleID(pattern, new TriplesListIterator(this));
		}
	}
	
	public boolean isSorted() {
		return this.sorted;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see hdt.triples.Triples#searchAll()
	 */
	@Override
	public IteratorTripleID searchAll() {
		TripleID all = new TripleID(0, 0, 0);
		return this.search(all);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see hdt.triples.Triples#getNumberOfElements()
	 */
	@Override
	public long getNumberOfElements() {
		return numValidTriples.longValue();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see hdt.triples.Triples#size()
	 */
	@Override
	public long size() {
		return this.getNumberOfElements() * TripleID.size();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see hdt.triples.Triples#save(java.io.OutputStream)
	 */
	@Override
	public void save(OutputStream output, ControlInfo controlInformation, ProgressListener listener)
			throws IOException {
		controlInformation.clear();
		controlInformation.setInt("numTriples", numValidTriples.longValue());
		controlInformation.setFormat(HDTVocabulary.TRIPLES_TYPE_TRIPLESLIST);
		controlInformation.setInt("order", order.ordinal());
		controlInformation.save(output);

		DataOutputStream dout = new DataOutputStream(output);
		int count = 0;
		for (TripleID triple : arrayOfTriples) {
			if (triple.isValid()) {
				// FIXME: writeLong ??
				dout.writeInt((int) triple.getSubject());
				dout.writeInt((int) triple.getPredicate());
				dout.writeInt((int) triple.getObject());
				ListenerUtil.notifyCond(listener, "Saving TriplesList", count, arrayOfTriples.size64());
			}
			count++;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see hdt.triples.Triples#load(java.io.InputStream)
	 */
	@Override
	public void load(InputStream input, ControlInfo controlInformation, ProgressListener listener) throws IOException {
		order = TripleComponentOrder.values()[(int) controlInformation.getInt("order")];
		long totalTriples = controlInformation.getInt("numTriples");

		int numRead = 0;

		while (numRead < totalTriples) {
			arrayOfTriples.add(new TripleID(IOUtil.readLong(input), IOUtil.readLong(input), IOUtil.readLong(input)));
			numRead++;
			numValidTriples.incrementAndGet();
			ListenerUtil.notifyCond(listener, "Loading TriplesList", numRead, totalTriples);
		}

		sorted = false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see hdt.triples.Triples#load(hdt.triples.TempTriples)
	 */
	@Override
	public void load(TempTriples input, ProgressListener listener) {
		IteratorTripleID iterator = input.searchAll();
		while (iterator.hasNext()) {
			arrayOfTriples.add(iterator.next());
			numValidTriples.incrementAndGet();
		}

		sorted = false;
	}

	/**
	 * @param order the order to set
	 */
	public void setOrder(TripleComponentOrder order) {
		if (this.order.equals(order))
			return;
		this.order = order;
		sorted = false;
	}

	@Override
	public TripleComponentOrder getOrder() {
		return order;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see hdt.triples.TempTriples#insert(hdt.triples.TripleID[])
	 */
	@Override
	public boolean insert(TripleID... triples) {
		for (TripleID triple : triples) {
			arrayOfTriples.add(new TripleID(triple));
			numValidTriples.incrementAndGet();
		}
		sorted = false;
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see hdt.triples.TempTriples#insert(int, int, int)
	 */
	@Override
	public boolean insert(long subject, long predicate, long object) {
		arrayOfTriples.add(new TripleID(subject, predicate, object));
		numValidTriples.incrementAndGet();
		sorted = false;
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see hdt.triples.TempTriples#delete(hdt.triples.TripleID[])
	 */
	@Override
	public boolean remove(TripleID... patterns) {
		boolean removed = false;
		for (TripleID triple : arrayOfTriples) {
			for (TripleID pattern : patterns) {
				if (triple.match(pattern)) {
					triple.clear();
					removed = true;
					numValidTriples.decrementAndGet();
					break;
				}
			}
		}

		return removed;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see hdt.triples.TempTriples#sort(datatypes.TripleComponentOrder)
	 */
	@Override
	public void sort(ProgressListener listener) {
		logger.info("Sorting triples");
		if (!sorted) {
			arrayOfTriples.sort(TripleIDComparator.getComparator(order));
		}
		sorted = true;
		logger.info("Triples sorted");
	}

	/**
	 * If called while triples not sorted nothing will happen!
	 */
	@Override
	public void removeDuplicates(ProgressListener listener) {
		if (arrayOfTriples.size64() <= 1 || !sorted) {
			return;
		}

		if (order == TripleComponentOrder.Unknown || !sorted) {
			throw new IllegalArgumentException("Cannot remove duplicates unless sorted");
		}

		logger.info("Removing duplicates");
		logger.info("Number of triples " + arrayOfTriples.size64());

		long j = 0;

		for (long i = 1; i < arrayOfTriples.size64(); i++) {
			if (arrayOfTriples.get(i).compareTo(arrayOfTriples.get(j)) != 0) {
				j++;
				arrayOfTriples.set(j, arrayOfTriples.get(i));
			}
			ListenerUtil.notifyCond(listener, "Removing duplicate triples", i, arrayOfTriples.size64());
		}

		logger.info("Number of triples " + arrayOfTriples.size64());

		while (arrayOfTriples.size64() > j + 1) {
			arrayOfTriples.remove(arrayOfTriples.size64() - 1);
		}
		numValidTriples.set(j + 1);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "TriplesList [" + arrayOfTriples + "\n order=" + order + "]";
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see hdt.triples.Triples#populateHeader(hdt.header.Header, java.lang.String)
	 */
	@Override
	public void populateHeader(Header header, String rootNode) {
		header.insert(rootNode, HDTVocabulary.TRIPLES_TYPE, HDTVocabulary.TRIPLES_TYPE_TRIPLESLIST);
		header.insert(rootNode, HDTVocabulary.TRIPLES_NUM_TRIPLES, getNumberOfElements());
		header.insert(rootNode, HDTVocabulary.TRIPLES_ORDER, order.ordinal());
	}

	public String getType() {
		return HDTVocabulary.TRIPLES_TYPE_TRIPLESLIST;
	}

	@Override
	public void generateIndex(ProgressListener listener) {

	}

	@Override
	public void loadIndex(InputStream input, ControlInfo ci, ProgressListener listener) throws IOException {

	}

	@Override
	public void saveIndex(OutputStream output, ControlInfo ci, ProgressListener listener) throws IOException {

	}

	@Override
	public void clear() {
		this.arrayOfTriples.clear();
		this.numValidTriples = new AtomicLong(0L);
		this.order = TripleComponentOrder.Unknown;
		sorted = false;
	}

	@Override
	public void load(Triples triples, ProgressListener listener) {
		this.clear();
		IteratorTripleID it = triples.searchAll();
		while (it.hasNext()) {
			TripleID triple = it.next();
			this.insert(triple.getSubject(), triple.getPredicate(), triple.getObject());
		}
		sorted = false;
	}

	@Override
	public void close() throws IOException {
		this.arrayOfTriples.close();
	}

	/**
	 * Iterator implementation to iterate over a TriplesList object
	 * 
	 * @author mario.arias
	 *
	 */
	public class TriplesListIterator implements IteratorTripleID {

		private RocksTriples triplesList;
		private int pos;

		public TriplesListIterator(RocksTriples triplesList) {
			this.triplesList = triplesList;
			this.pos = 0;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see hdt.iterator.IteratorTripleID#hasNext()
		 */
		@Override
		public boolean hasNext() {
			return pos < triplesList.getNumberOfElements();
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see hdt.iterator.IteratorTripleID#next()
		 */
		@Override
		public TripleID next() {
			return triplesList.arrayOfTriples.get((int) pos++);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see hdt.iterator.IteratorTripleID#hasPrevious()
		 */
		@Override
		public boolean hasPrevious() {
			return pos > 0;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see hdt.iterator.IteratorTripleID#previous()
		 */
		@Override
		public TripleID previous() {
			return triplesList.arrayOfTriples.get((int) --pos);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see hdt.iterator.IteratorTripleID#goToStart()
		 */
		@Override
		public void goToStart() {
			pos = 0;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see hdt.iterator.IteratorTripleID#estimatedNumResults()
		 */
		@Override
		public long estimatedNumResults() {
			return triplesList.getNumberOfElements();
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see hdt.iterator.IteratorTripleID#numResultEstimation()
		 */
		@Override
		public ResultEstimationType numResultEstimation() {
			return ResultEstimationType.EXACT;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see hdt.iterator.IteratorTripleID#canGoTo()
		 */
		@Override
		public boolean canGoTo() {
			return true;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see hdt.iterator.IteratorTripleID#goTo(int)
		 */
		@Override
		public void goTo(long pos) {
			this.pos = (int) pos;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see hdt.iterator.IteratorTripleID#getOrder()
		 */
		@Override
		public TripleComponentOrder getOrder() {
			return triplesList.getOrder();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	@Override
	public void mapFromFile(CountInputStream in, File f, ProgressListener listener) throws IOException {
		throw new NotImplementedException();
	}

	@Override
	public void mapIndex(CountInputStream input, File f, ControlInfo ci, ProgressListener listener) throws IOException {
	}

	@Override
	public void replaceAllIds(DictionaryIDMapping mapSubj, DictionaryIDMapping mapPred, DictionaryIDMapping mapObj) {
		sorted = false;
		for (TripleID triple : arrayOfTriples) {
			//TODO parallelize
			triple.setAll(mapSubj.getNewID(triple.getSubject() - 1), mapPred.getNewID(triple.getPredicate() - 1),
					mapObj.getNewID(triple.getObject() - 1));
		}
	}

}
