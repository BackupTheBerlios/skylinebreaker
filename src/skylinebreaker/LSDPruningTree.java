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


public class LSDPruningTree extends LSDAbstractTree {

	public final static class LSDPruningTreeFactory implements LSDAbstractTreeFactory
	{
		@Override
		public LSDAbstractTree get(final ScoringFunction scoringFunction, final InputData firstElement) {
			return new LSDPruningTree(scoringFunction, firstElement);
		}	
	}
	
	
	public LSDPruningTree(final ScoringFunction scoringFunction, final InputData firstElement)
	{
		super(scoringFunction, firstElement);
		pruningLevel = scoringFunction.evaluate(firstElement).getPruningLevel();
		//computeOverallLevel(firstElement); // needed to index this element with the levels mapping
	}
	
	private double pruningLevel;
	@Override
	public void add(final InputData element)
	{
		final FeatureVector element_level = scoringFunction.evaluate(element);
		final double element_overalllevel = element_level.overallLevel; //computeOverallLevel(element); TODO
		if(pruningLevel < element_overalllevel) return;
		final double element_pruningLevel = element_level.getPruningLevel();
		if(element_pruningLevel < pruningLevel) pruningLevel = element_pruningLevel;
		
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
	/*
	public int computeOverallLevel(final FlatLevelCombination element)
	{
		Integer[] l = new Integer[dimensions];
		int level = 0;
		for(int i = 0; i < dimensions; ++i)
		{
			l[i] = (int) scoringFunction.getBasePref(i).level(scoringFunction.getAttributeSelector(i).evaluate(element, null, scoringFunction.getBasePref(i).getDomainType()), null);
			level += l[i];
		}
		levels.put(element.getIdentifier(), l);
		return level;
	}
	
	
	final private HashMap<Integer,Integer[]> levels = new HashMap<Integer,Integer[]>();
	
	@Override
	public int getLevel(final FlatLevelCombination element, int i)
	{
		return levels.get(element.getIdentifier())[i];
	}
	
	@Override
	public int getOverallLevel(final FlatLevelCombination element)
	{
		int level = 0;
		for(int i = 0; i < dimensions; ++i)
			level += getLevel(element,i);
		return level;
		
	}
	*/
	
}
