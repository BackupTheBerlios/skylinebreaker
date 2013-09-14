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

import java.util.ArrayList;

import compat.InputData;
import compat.ScoringFunction;

import skylinebreaker.LSDAbstractTree.BucketNode;
import skylinebreaker.LSDAbstractTree.DirectoryNode;
import skylinebreaker.LSDAbstractTree.Node;

public class SBSingleCNN {
	
	
	private final LSDAbstractTree tree;
	static SBBase.NearestNeighborOfZero nnzero = null;
	final int processes;
	public SBSingleCNN(final LSDAbstractTree tree, int processes)
	{
		this.tree = tree;
		this.processes = processes;
		
	}
	
	public InputData[] computeSkyline(ScoringFunction scoringFunction) throws InterruptedException
	{
		final LSDAbstractTree.Node root = tree.getRoot();
		Node n = root;
		int depth = 0;
		
		while( !(n instanceof BucketNode))
		{
			DirectoryNode nc = (DirectoryNode) n;
			n = nc.left;
			++depth;
		}
		final BucketNode bn = (BucketNode) n;
		InputData[] zeroLocalSkyline = bn.computeLocalSkyline();
		{
			int nearestToZeroNorm = Integer.MAX_VALUE;
			InputData nearestToZero = null;
			for(final InputData element : zeroLocalSkyline)
			{
				final int l1n = tree.getOverallLevel(element);
				if(nearestToZeroNorm > l1n) { nearestToZero = element; nearestToZeroNorm = l1n; }
			}
			synchronized(nnzero)
			{
				if(nnzero.norm > nearestToZeroNorm) 
				{
					nnzero.norm = nearestToZeroNorm;
					nnzero.neighbor = nearestToZero;
				}
				++nnzero.threadCount;
				nnzero.notifyAll();
				while(nnzero.threadCount < processes)
					nnzero.wait();
				//if(tree instanceof LSDPruningTree) ((LSDPruningTree)tree).computeOverallLevel(nnzero.neighbor); // hash the value of nnzero TODO
			}
		}
		
		ArrayList<InputData[]> localSkylines = new ArrayList<InputData[]>();

		for(int i = 0; i < tree.dimensions-1; ++i)
		{
			final DirectoryNode dn = n.getParent(); --depth;
			if(dn == null) break;
			localSkylines.add(SBSingle.computeBucketSkyline(scoringFunction, dn.left == n ? dn.right : dn.left));
			n = dn;
			
		}
		
		if(n != root)
		{
			while(n != root)
			{
				final DirectoryNode dn = n.getParent(); --depth;
				assert (dn.right != n) : "Upward going not on the left side";
				
				if(dn.right instanceof BucketNode) localSkylines.add(SBSingle.computeBucketSkyline(scoringFunction, dn.right));
				else
				{
					final int[] minlevels = new int[tree.dimensions];
					final int dim = tree.getSplitAxis(depth);
					minlevels[dim] = dn.location;
					pruneBuckets( (DirectoryNode)dn.right, depth+1, minlevels, localSkylines);
				}
				
				n = dn;
				//minlevels[getSplitAxis(depth)] = 0;
				
			}
		}
		for(final InputData[] localSkyline : localSkylines)
			zeroLocalSkyline = SBBase.combineLocalSkyline(scoringFunction, localSkyline, zeroLocalSkyline);
		return zeroLocalSkyline;
	}
	
	private void pruneBuckets(final LSDAbstractTree.DirectoryNode dn, final int depth, final int[] minlevels, ArrayList<InputData[]> localSkylines)
	{
		final int dim = tree.getSplitAxis(depth);
		final int oldvalue = minlevels[dim]; 
		minlevels[dim] = dn.location;
		boolean greater = true;
		for(int i = 0; i < tree.dimensions; ++i) if(minlevels[i] <= tree.getLevel(nnzero.neighbor, i)) { greater = false; break; }
		minlevels[dim] = oldvalue;
		if(dn.left instanceof LSDAbstractTree.BucketNode) localSkylines.add( ((LSDAbstractTree.BucketNode) dn.left).computeLocalSkyline());
		else pruneBuckets( ((LSDAbstractTree.DirectoryNode)dn.left), depth+1, minlevels, localSkylines);
		
		if(!greater){
			final int[] rightminlevels = minlevels.clone();
			rightminlevels[dim] = dn.location;
			if(dn.right instanceof LSDAbstractTree.BucketNode) localSkylines.add( ((LSDAbstractTree.BucketNode) dn.right).computeLocalSkyline());
			else pruneBuckets( ((LSDAbstractTree.DirectoryNode)dn.right), depth+1, rightminlevels, localSkylines);
		}
	
	}
	
	/*
	static InputData[] computeBucketSkyline(final Node n)
	{
		if(n instanceof BucketNode) return ((BucketNode)n).computeLocalSkyline();
		else 
		{
			final DirectoryNode dn = (DirectoryNode) n;
			return SBBase.combineLocalSkyline(computeBucketSkyline(dn.left),computeBucketSkyline(dn.right));
		}
	}
	*/

}
