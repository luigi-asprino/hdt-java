/**
 * File: $HeadURL$
 * Revision: $Rev$
 * Last modified: $Date$
 * Last modified by: $Author$
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
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

package org.rdfhdt.hdtjena;

import java.util.Map;

import org.rdfhdt.hdt.dictionary.Dictionary;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdtjena.bindings.HDTId;
import org.rdfhdt.hdtjena.cache.DictionaryCache;
import org.rdfhdt.hdtjena.cache.DictionaryCacheArray;
import org.rdfhdt.hdtjena.cache.DictionaryCacheLRI;
import org.rdfhdt.hdtjena.cache.DummyMap;

import com.hp.hpl.jena.graph.JenaNodeCreator;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleMatch;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.ARQConstants;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;

/**
 * Wraps all operations from ids to Nodes and vice versa using an HDT Dictionary.
 * 
 * @author mario.arias
 *
 */
public class NodeDictionary {

	private final Dictionary dictionary;

	private final DictionaryCache cacheIDtoNode [] = new DictionaryCache[TripleComponentRole.values().length];
	
	@SuppressWarnings("unchecked")
	Map<String, Integer> cacheNodeToId [] = new Map[TripleComponentRole.values().length];
	
	public NodeDictionary(Dictionary dictionary) {
		this.dictionary = dictionary;
		
		// ID TO NODE	
		final int idToNodeSize = 20000;
		if(dictionary.getNsubjects()>idToNodeSize) {			
			cacheIDtoNode[0] = new DictionaryCacheLRI(idToNodeSize);
		} else {
			cacheIDtoNode[0] = new DictionaryCacheArray((int) dictionary.getNsubjects());
		}
		
		if(dictionary.getNpredicates()>idToNodeSize) {
			cacheIDtoNode[1] = new DictionaryCacheLRI(idToNodeSize);
		} else {	
			cacheIDtoNode[1] = new DictionaryCacheArray((int) dictionary.getNpredicates());
		}
		
		if(dictionary.getNobjects()>idToNodeSize) {			
			cacheIDtoNode[2] = new DictionaryCacheLRI(idToNodeSize);
		} else {
			cacheIDtoNode[2] = new DictionaryCacheArray((int) dictionary.getNobjects());
		}
		
		// NODE TO ID
		// Disabled, it does not make so much impact.
		cacheNodeToId[0] = new DummyMap<String, Integer>();
		cacheNodeToId[1] = new DummyMap<String, Integer>();
		cacheNodeToId[2] = new DummyMap<String, Integer>();
		
//		final int nodeToIDSize = 1000;
//		
//		cacheNodeToId[0] = new LRUCache<String, Integer>(nodeToIDSize);
//		if(dictionary.getNpredicates()>nodeToIDSize) {
//			System.out.println("Predicates LRU");
//			cacheNodeToId[1] = new LRUCache<String, Integer>(nodeToIDSize);
//		} else {
//			System.out.println("Predicates Map");
//			cacheNodeToId[1] = new ConcurrentHashMap<String, Integer>((int) dictionary.getNpredicates());
//		}
//		cacheNodeToId[2] = new LRUCache<String, Integer>(nodeToIDSize);

	}

	public Node getNode(HDTId hdtid) {
		return getNode(hdtid.getId(), hdtid.getRole());
	}
	
	public Node getNode(int id, TripleComponentRole role) {
		Node node = cacheIDtoNode[role.ordinal()].get(id);
		if(node==null) {
			CharSequence str = dictionary.idToString(id, role);
			char firstChar = str.charAt(0);
			
			if(firstChar=='_') {
				node = JenaNodeCreator.createAnon(str);
			} else if(firstChar=='"') {
				node = JenaNodeCreator.createLiteral(str);
			} else {
				node = JenaNodeCreator.createURI(str);
			}
			
			cacheIDtoNode[role.ordinal()].put(id, node);
		}
		
		return node;
	}
	
	public int getIntID(Node node, TripleComponentRole role) {
		return getIntID(nodeToStr(node), role);
	}
	
	public int getIntID(Node node, PrefixMapping map, TripleComponentRole role) {
		return getIntID(nodeToStr(node, map), role);
	}
	
	public int getIntID(String str, TripleComponentRole role) {
		Integer intValue = cacheNodeToId[role.ordinal()].get(str);
		if(intValue!=null) {
			return intValue.intValue();
		}
		
		int val = dictionary.stringToId(str, role);
		cacheNodeToId[role.ordinal()].put(str, val);
		return val;
	}
	
	public static String nodeToStr(Node node, PrefixMapping map) {
		if(node.isURI()) {
			return map.expandPrefix(node.getURI());
		} else if(node.isVariable()) {
			return "";
		} else {
			return node.toString();
		}
	}
	
	public static String nodeToStr(Node node) {
		if(node==null || node.isVariable()) {
			return "";
		}else if(node.isURI()) {
			return node.getURI();
		} else {
			return node.toString();
		}
	}
	
	public TripleID getTripleID(Triple triple, PrefixMapping map) {
		return new TripleID(
				getIntID(nodeToStr(triple.getSubject(), map), TripleComponentRole.SUBJECT),
				getIntID(nodeToStr(triple.getPredicate(), map), TripleComponentRole.PREDICATE),
				getIntID(nodeToStr(triple.getObject(), map), TripleComponentRole.OBJECT)
				);
	}
	
	public TripleID getTriplePatID(TripleMatch jenaTriple) {
		int subject=0, predicate=0, object=0;
		
		if(jenaTriple.getMatchSubject()!=null) {
			subject = getIntID(jenaTriple.getMatchSubject(), TripleComponentRole.SUBJECT);
		}

		if(jenaTriple.getMatchPredicate()!=null) {
			predicate = getIntID(jenaTriple.getMatchPredicate().toString(), TripleComponentRole.PREDICATE);
		}

		if(jenaTriple.getMatchObject()!=null) {
			object = getIntID(jenaTriple.getMatchObject().toString(), TripleComponentRole.OBJECT);
		}
		return new TripleID(subject, predicate, object);
	}
	
	public static PrefixMapping getMapping(ExecutionContext ctx) {
		Query query = (Query) ctx.getContext().get(ARQConstants.sysCurrentQuery);		
		return query.getPrefixMapping();
	}

    public static final Var asVar(Node node)
    {
        if ( Var.isVar(node) )
            return Var.alloc(node) ;
        return null ;
    }


}
