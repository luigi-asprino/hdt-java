package org.rdfhdt.hdt.stream;

import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;

import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.iterator.DictionaryTranslateIteratorBufferParallel;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;

public class TripleStreamSupport {
	
	public static Stream<TripleString> search(HDT hdt, CharSequence s, CharSequence p, CharSequence o)
			throws NotFoundException{
		IteratorTripleString its = hdt.search(s, p, o);
		if(its instanceof Spliterator) {
			System.out.println("Slitp");
			return java.util.stream.StreamSupport.stream((DictionaryTranslateIteratorBufferParallel)its, true);
		}else {
			return java.util.stream.StreamSupport.stream(Spliterators.spliteratorUnknownSize(its, Spliterator.SIZED), true);
		}
		
	}

}
