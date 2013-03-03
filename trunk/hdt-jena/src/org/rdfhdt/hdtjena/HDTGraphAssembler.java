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

import static com.hp.hpl.jena.sparql.util.graph.GraphUtils.getStringValue;

import java.io.IOException;

import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;

import com.hp.hpl.jena.assembler.Assembler;
import com.hp.hpl.jena.assembler.Mode;
import com.hp.hpl.jena.assembler.assemblers.AssemblerBase;
import com.hp.hpl.jena.assembler.exceptions.AssemblerException;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;

public class HDTGraphAssembler extends AssemblerBase implements Assembler {

	private static boolean initialized = false;

	public static void init() {
		if(initialized) {
			return;
		}

		initialized = true;

		Assembler.general.implementWith(HDTJenaConstants.tGraphHDT, new HDTGraphAssembler());
	}

	@Override
	public Model open(Assembler a, Resource root, Mode mode)
	{
		String file = getStringValue(root, HDTJenaConstants.pFileName) ;
		boolean loadInMemory = Boolean.parseBoolean(getStringValue(root, HDTJenaConstants.pKeepInMemory));
		try {
			// FIXME: Read more properties. Cache config?
			HDT hdt;
			if(loadInMemory) {
				hdt = HDTManager.loadIndexedHDT(file, null);				
			} else {
				hdt = HDTManager.mapIndexedHDT(file, null);
			}
			HDTGraph graph = new HDTGraph(hdt);
			return ModelFactory.createModelForGraph(graph);
		} catch (IOException e) {
			e.printStackTrace();
			throw new AssemblerException(root, "Error reading HDT file: "+file+" / "+e.toString());
		}
	}
}
