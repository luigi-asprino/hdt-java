package org.rdfhdt.hdt.dictionary;

public interface DictionaryIDMapping {

	void add(CharSequence str);

	void setNewID(long oldId, long newID);

	long getNewID(long oldId);

	CharSequence getString(long oldId);

	long size();

}