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

import java.util.Arrays;
import java.util.Comparator;

import compat.FeatureVector;
import compat.InputData;
import compat.ScoringFunction;

public abstract class LSDAbstractTree {
	final static int BUCKETSIZE = 1000;
	
	static interface Node
	{
		DirectoryNode getParent();
	}
	
	static public interface LSDAbstractTreeFactory
	{
		public abstract LSDAbstractTree get(final ScoringFunction scoringFunction, final InputData firstElement);
	}
	static class DirectoryNode implements Node
	{
		final int location;
		Node left;
		Node right;
		final DirectoryNode parent;
		
		DirectoryNode(final int location, final Node left, final Node right, final DirectoryNode parent)
		{
			this.location = location;
			this.left = left;	
			this.right = right;
			this.parent = parent;
		}

		@Override
		public DirectoryNode getParent() {
			return parent;
		}
	}
	class BucketNode implements Node
	{
		final InputData[] data = new InputData[BUCKETSIZE];
		int reserved = 0;
		final int axis;
		DirectoryNode parent = null;
		
		BucketNode(final int axis)
		{
			this.axis = axis;
		}
		void add(InputData el)
		{
			data[reserved++] = el;
		}
		boolean isFull()
		{
			return reserved+1 == data.length;
		}
		int splitPoint()
		{
			Arrays.sort(data, 0, reserved, new AxisProjectionComparator(axis));
			return data.length/2; //[data.length/2].getLevel(axis);
		}
		public InputData[] computeLocalSkyline()
		{
			for(int i = 0; i < reserved; ++i)
			{
				outerLoop:
				for(int j = i+1; j < reserved; ++j)
				{
					innerLoop:
					
					//if(data[j] == null) continue;
					switch(scoringFunction.evaluate(data[i]).compare(scoringFunction.evaluate(data[j]))) {
					case FeatureVector.LESS: 
						data[i--] = data[--reserved];
						break outerLoop;
					case FeatureVector.GREATER: 
						data[j--] = data[--reserved];
						break innerLoop;
					}
				}
			}
			return Arrays.copyOf(data, reserved);
		}
		
		@Override
		public DirectoryNode getParent() {
			return parent;
		}
	}
	
	Node getRoot()
	{
		return root;
	}
	
	protected Node root;
	public final int dimensions;
	public final ScoringFunction scoringFunction;
	public LSDAbstractTree(final ScoringFunction scoringFunction, final InputData firstElement)
	{
		this.scoringFunction = scoringFunction;
		this.dimensions = scoringFunction.getDimensionality();
		final BucketNode bn = new BucketNode(getSplitAxis(0));
		bn.add(firstElement);
		root = bn;
	}
	
	
	public int getLevel(final InputData element, int i)
	{
		//return element.getLevel(i);
		return (int) scoringFunction.getProjection(i).evaluate(element);
	}
	public int getOverallLevel(final InputData element)
	{
		//return element.getOverallLevel();
		
		int level = 0;
		for(int i = 0; i < dimensions; ++i)
			level += (int) scoringFunction.getProjection(i).evaluate(element);
		return level;
		
	}
	
	public abstract void add(final InputData element);
	
	
	int getSplitAxis(final int depth)
	{
		return depth % dimensions;
	}
	private final class AxisProjectionComparator implements Comparator<InputData> {
		final int axis;
		AxisProjectionComparator(int axis) { this.axis = axis; }
		
		 @Override
		 public int compare(final InputData a, final InputData b) { 
			 return Integer.compare(getLevel(a,axis), getLevel(b,axis));
		 }
	};
}
