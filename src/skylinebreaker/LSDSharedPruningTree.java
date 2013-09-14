/*
 * Copyright (c) 2013, University of Augsburg, Dominik Köppl <niki@users.berlios.de>
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 	* Redistributions of source code must retain the above copyright
 * 	  notice, this list of conditions and the following disclaimer.
 * 	* Redistributions in binary form must reproduce the above copyright
 * 	  notice, this list of conditions and the following disclaimer in the
 * 	  documentation and/or other materials provided with the distribution.
 * 	* Neither the name of the <organization> nor the
 * 	  names of its contributors may be used to endorse or promote products
 * 	  derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package skylinebreaker;

import compat.FeatureVector;
import compat.InputData;
import compat.ScoringFunction;



public final class LSDSharedPruningTree extends LSDPruningTree {

	public final static class LSDSharedPruningTreeFactory implements LSDAbstractTreeFactory
	{
		public LSDSharedPruningTreeFactory()
		{
			LSDSharedPruningTree.resetPruningLevel();
		}
		@Override
		public LSDAbstractTree get(final ScoringFunction scoringFunction, final InputData firstElement) {
			return new LSDSharedPruningTree(scoringFunction, firstElement);
		}	
	}
	
	
	public LSDSharedPruningTree(final ScoringFunction scoringFunction, final InputData firstElement) 
	{
		super(scoringFunction, firstElement);
		final double element_pruningLevel = scoringFunction.evaluate(firstElement).getPruningLevel();
		synchronized(globalPruningLevel)
		{
			if(element_pruningLevel < globalPruningLevel) globalPruningLevel = element_pruningLevel;
		}
	}
	
	private static void resetPruningLevel() { globalPruningLevel = Double.MAX_VALUE; }
	
	private static Double globalPruningLevel = Double.MAX_VALUE;
	@Override
	public void add(final InputData element)
	{
		final FeatureVector element_level = scoringFunction.evaluate(element);
		final double element_overalllevel = element_level.overallLevel; // computeOverallLevel(element); TODO
		synchronized(globalPruningLevel)
		{
			if(globalPruningLevel < element_overalllevel) return;
			final double element_pruningLevel = element_level.getPruningLevel();
			if(element_pruningLevel < globalPruningLevel) globalPruningLevel = element_pruningLevel;	
		}
		
		
		Node n = root;
		int depth = 0;
		while( !(n instanceof BucketNode))
		{
			final DirectoryNode nc = (DirectoryNode) n;
			final int compared = Integer.compare(getLevel(element,getSplitAxis(depth++)), nc.location);
			n = compared > 0 ? nc.right : nc.left;
		}
		final BucketNode bn = (BucketNode) n;
		if(bn.isFull())
		{
			bn.add(element);
			final int location = bn.splitPoint();
			final BucketNode lesser = new BucketNode(getSplitAxis(depth+1));
			final BucketNode higher = new BucketNode(getSplitAxis(depth+1));
			for(int i = 0; i < location; ++i) lesser.add(bn.data[i]);
			for(int i = location; i < bn.data.length; ++i) higher.add(bn.data[i]);
			final DirectoryNode dn = lesser.parent = higher.parent = new DirectoryNode(scoringFunction.evaluate(bn.data[location]).getLevel(getSplitAxis(depth)), lesser, higher, bn.parent);
			if(bn.parent == null) root = dn;
			else
			{
				if(bn.parent.left == bn) bn.parent.left = dn;
				else bn.parent.right = dn;
			}
		}
		else
			bn.add(element);
	}
	
	
}
