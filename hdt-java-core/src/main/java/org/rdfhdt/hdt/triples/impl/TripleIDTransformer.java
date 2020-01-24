package org.rdfhdt.hdt.triples.impl;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

import org.rdfhdt.hdt.triples.TripleID;

import it.cnr.istc.stlab.rocksmap.transformer.RocksTransformer;

public class TripleIDTransformer implements RocksTransformer<TripleID> {

	public byte[] transform(TripleID value) {
		final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 3);
		buffer.putLong(0, value.getSubject());
		buffer.putLong(Long.BYTES, value.getPredicate());
		buffer.putLong(Long.BYTES * 2, value.getObject());
		return buffer.array();

	}

	@Override
	public TripleID transform(byte[] value) {
		final byte[] s = Arrays.copyOfRange(value, 0, Long.BYTES);
		final byte[] p = Arrays.copyOfRange(value, Long.BYTES, 2 * Long.BYTES);
		final byte[] o = Arrays.copyOfRange(value, 2 * Long.BYTES, 3 * Long.BYTES);
		return new TripleID(bytesToLong(s), bytesToLong(p), bytesToLong(o));
	}

	private long bytesToLong(byte[] bytes) {
		final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
		buffer.put(bytes);
		buffer.flip();// need flip
		return buffer.getLong();
	}

	@Override
	public Collection<TripleID> transformCollection(byte[] value) {
		throw new UnsupportedOperationException();
	}

}
