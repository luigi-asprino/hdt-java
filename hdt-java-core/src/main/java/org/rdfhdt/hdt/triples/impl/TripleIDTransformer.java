package org.rdfhdt.hdt.triples.impl;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

import org.rdfhdt.hdt.triples.TripleID;

import it.cnr.istc.stlab.rocksmap.transformer.RocksTransformer;

public class TripleIDTransformer implements RocksTransformer<TripleID> {

	public byte[] transform(TripleID value) {
		ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 3);
		buffer.putLong(0, value.getSubject());
		buffer.putLong(Long.BYTES, value.getPredicate());
		buffer.putLong(Long.BYTES * 2, value.getObject());
		return buffer.array();

	}

	@Override
	public TripleID transform(byte[] value) {
		byte[] s = Arrays.copyOfRange(value, 0,Long.BYTES);
		byte[] p = Arrays.copyOfRange(value, Long.BYTES, 2 * Long.BYTES);
		byte[] o = Arrays.copyOfRange(value, 2 * Long.BYTES, 3 * Long.BYTES);
		return new TripleID(bytesToLong(s), bytesToLong(p), bytesToLong(o));
	}

	private long bytesToLong(byte[] bytes) {
		ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
		buffer.put(bytes);
		buffer.flip();// need flip
		return buffer.getLong();
	}

	@Override
	public Collection<TripleID> transformCollection(byte[] value) {
		throw new UnsupportedOperationException();
	}
	
//	public static void main(String[] args) {
//		
//		
//		
//		TripleID t = new TripleID(1, 2, 3);
//		TripleIDTransformer tran = new TripleIDTransformer();
//		TripleID tr =tran.transform(tran.transform(t));
//		System.out.println(tr.getSubject());
//		System.out.println(tr.getPredicate());
//		System.out.println(tr.getObject());
//	}

}
