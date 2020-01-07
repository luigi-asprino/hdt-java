package org.rdfhdt.hdt.dictionary.impl.section;

import java.util.Collection;

import org.rdfhdt.hdt.util.string.CompactString;

import it.cnr.istc.stlab.rocksmap.transformer.RocksTransformer;

public class CompactStringRocksTransformer implements RocksTransformer<CompactString> {

	public byte[] transform(CompactString value) {
		return value.getData();
	}

	@Override
	public CompactString transform(byte[] value) {
		return new CompactString(new String(value));
	}

	@Override
	public Collection<CompactString> transformCollection(byte[] value) {
		throw new UnsupportedOperationException();
	}

}
