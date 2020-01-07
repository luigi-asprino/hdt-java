package org.rdfhdt.hdt.dictionary.impl.section;

import java.util.Collection;

import it.cnr.istc.stlab.rocksmap.transformer.RocksTransformer;

public class CharSequenceRocksTransformer implements RocksTransformer<CharSequence> {

	public byte[] transform(CharSequence value) {
		return value.toString().getBytes();
	}

	@Override
	public CharSequence transform(byte[] value) {
		return new String(value);
	}

	@Override
	public Collection<CharSequence> transformCollection(byte[] value) {
		throw new UnsupportedOperationException();
	}

}
