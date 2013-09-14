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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.RecursiveTask;
import java.util.concurrent.TimeUnit;

import skylinebreaker.LSDAbstractTree.LSDAbstractTreeFactory;
import skylinebreaker.SBBase.NearestNeighborOfZero;

import compat.InputData;
import compat.ScoringFunction;

public class SBFork {

	final private ForkJoinPool forkPool;
	
	final LSDAbstractTreeFactory treeFactory;
	final ScoringFunction scoringFunction;
	final Iterator<InputData> inputIterator;
	final int processes;
	public SBFork(final int processes, final LSDAbstractTreeFactory treeFactory, final Iterator<InputData> inputIterator, final ScoringFunction scoringFunction)
	{
		forkPool = new ForkJoinPool(processes);
		this.processes = processes;
		this.inputIterator = inputIterator;
		this.scoringFunction = scoringFunction;
		this.treeFactory = treeFactory;

	}
	public InputData[] compute() throws InterruptedException
	{
		
		final LSDWorker[] threads = new LSDWorker[processes];
		for(int i = 0; i < processes; ++i) threads[i] = new LSDWorker(i, (InputData) inputIterator.next());
		for(int i = 0; i < processes; ++i) forkPool.submit(threads[i]);
		
		while(forkPool.getQueuedSubmissionCount() > 0 || !forkPool.isQuiescent()) 
		{
			Thread.sleep(10);
			InputData[] a = null, b = null;
			
			int queueSize;
			do
			{
				
			    synchronized(localSkylineQueue) 
			    {
			    	queueSize = localSkylineQueue.size();
			    	//if(queueSize > 1) System.out.println("Found Queue-Size: " + queueSize);
			    	if(queueSize > 1)
			    	{
			    		a = localSkylineQueue.poll();
			    		b = localSkylineQueue.poll();
			    	}
			    	
			    }
			    
			    if(a != null) forkPool.submit(new SkylineMergeWorker(a, b));
			}
			while(queueSize > 3);
		}
		forkPool.shutdown();
		forkPool.awaitTermination(1, TimeUnit.MINUTES);
		
		//System.out.println("Final Queue-Size: " + localSkylineQueue.size()); TODO
		if(localSkylineQueue.size() == 2) return SBBase.combineLocalSkyline(scoringFunction, localSkylineQueue.poll(), localSkylineQueue.poll());
		
		
		//forkPool.awaitTermination(2, TimeUnit.MINUTES);
		//forkPool.getRunningThreadCount();
		//forkPool.
		assert localSkylineQueue.size() == 1;
		return localSkylineQueue.poll();
	}

	

	NearestNeighborOfZero nnzero = new NearestNeighborOfZero();
	
	
	final class LSDWorker implements Runnable
	{
		InputData[] data = null;
		private final InputData init_value;
		LSDAbstractTree tree;
		final int number;
		
		
		
		public LSDWorker(final int number, final InputData init_value)
		{
			this.number = number;
			this.init_value = init_value;
		}

		@Override
		public void run()
    	{
			try {
				tree = treeFactory.get(scoringFunction, init_value);
				while(true)
	    		{
	    			InputData element;
	    			synchronized(inputIterator)
	    			{
	    				if(!inputIterator.hasNext()) break;
	    				element = (InputData) inputIterator.next();
	    			}
	    			tree.add(element);
	    		}
				final LSDAbstractTree.Node root = tree.getRoot();
				LSDAbstractTree.Node n = root;
				int depth = 0;
				
				while( !(n instanceof LSDAbstractTree.BucketNode))
				{
					LSDAbstractTree.DirectoryNode nc = (LSDAbstractTree.DirectoryNode) n;
					n = nc.left;
					++depth;
				}
				final LSDAbstractTree.BucketNode bn = (LSDAbstractTree.BucketNode) n;
				InputData[] zeroLocalSkyline = bn.computeLocalSkyline();
				{
					int nearestToZeroNorm = Integer.MAX_VALUE;
					InputData nearestToZero = null;
					for(final InputData element : zeroLocalSkyline)
					{
						final int l1n = tree.getOverallLevel(element);
						if(nearestToZeroNorm > l1n) { nearestToZero = element; nearestToZeroNorm = l1n; }
					}
				    synchronized(localSkylineQueue) { localSkylineQueue.add(zeroLocalSkyline); }
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
					}
				}
				
			
				for(int i = 0; i < tree.dimensions-1; ++i)
				{
					final LSDAbstractTree.DirectoryNode dn = n.getParent(); --depth;
					if(dn == null) break;
					if(dn.right instanceof LSDAbstractTree.BucketNode) 
						submit(new BucketNodeWorker(true, (LSDAbstractTree.BucketNode) dn.right));
					else submit(new DirectoryNodeWorker(true, (LSDAbstractTree.DirectoryNode) dn.right));
					n = dn;
				}
				
				
				if(n != root)
				{
					while(n != root)
					{
						final LSDAbstractTree.DirectoryNode dn = n.getParent(); --depth;
						assert (dn.right != n) : "Upward going not on the left side";
						
						if(dn.right instanceof LSDAbstractTree.BucketNode) submit(new BucketNodeWorker(true, (LSDAbstractTree.BucketNode) dn.right));
						else
						{
							final int[] minlevels = new int[tree.dimensions];
							final int dim = tree.getSplitAxis(depth);
							minlevels[dim] = dn.location;
							submit(new PruneBucketWorker( (LSDAbstractTree.DirectoryNode)dn.right, depth+1, minlevels));
						}
						n = dn;				
					}
				}
				
			} 
			catch (InterruptedException e) 
			{
				e.printStackTrace();
			}
    	}
		

		

		class PruneBucketWorker extends RecursiveAction
		{
			private static final long serialVersionUID = 3191824942893677558L;
			final LSDAbstractTree.DirectoryNode dn;
			final int depth;
			final int[] minlevels;
			private PruneBucketWorker(final LSDAbstractTree.DirectoryNode dn, final int depth, final int[] minlevels)
			{
				this.dn = dn;
				this.depth = depth;
				this.minlevels = minlevels;
			}

			@Override
			protected void compute() {
				final int dim = tree.getSplitAxis(depth);
				final int oldvalue = minlevels[dim]; 
				minlevels[dim] = dn.location;
				boolean greater = true;
				for(int i = 0; i < tree.dimensions; ++i) if(minlevels[i] <= tree.getLevel(nnzero.neighbor,i)) { greater = false; break; }
				minlevels[dim] = oldvalue;
				if(dn.left instanceof LSDAbstractTree.BucketNode) submit(new BucketNodeWorker(true, (LSDAbstractTree.BucketNode) dn.left));
				else submit(new PruneBucketWorker( ((LSDAbstractTree.DirectoryNode)dn.left), depth+1, minlevels));
				
				if(!greater)
				{
					final int[] rightminlevels = minlevels.clone();
					rightminlevels[dim] = dn.location;
					if(dn.right instanceof LSDAbstractTree.BucketNode) submit(new BucketNodeWorker(true, (LSDAbstractTree.BucketNode) dn.right));
					else submit(new PruneBucketWorker( ((LSDAbstractTree.DirectoryNode)dn.right), depth+1, rightminlevels));
				}
			}
		}
		
		
		
	}				
	private final Queue<InputData[]> localSkylineQueue = new LinkedList<InputData[]>();

	public void addLocalSkyline(final InputData[] localSkyline)
	{
		InputData[] a = null;
	    synchronized(localSkylineQueue) 
	    {
	    	if(localSkylineQueue.isEmpty()) localSkylineQueue.add(localSkyline);
	    	else a = localSkylineQueue.poll();
	    }
	    if(a != null) submit(new SkylineMergeWorker(a, localSkyline));
	}
	
	
	private void submit(ForkJoinTask<?> task) {
		synchronized(forkPool) { forkPool.submit(task); }
		
	}
	private void submit(Runnable task) {
		synchronized(forkPool) { forkPool.submit(task); }
	}


	class SkylineMergeWorker implements Runnable
	{
		
		final InputData[] a;
		final InputData[] b;
		
		public SkylineMergeWorker(final InputData[] a, final InputData[] b) 
		{
			this.a = a;
			this.b = b;
		}

		@Override
		public void run() 
		{
			addLocalSkyline(SBBase.combineLocalSkyline(scoringFunction, a, b));
		}
		
	}
	
class BucketNodeWorker extends ForkJoinTask<InputData[]> 
{
	private static final long serialVersionUID = 3788728179104943691L;
	final LSDAbstractTree.BucketNode node;
	private InputData[] result = null;
	private final boolean notify; 
	BucketNodeWorker(final boolean notify, LSDAbstractTree.BucketNode node)
	{
		this.notify = notify;
		this.node = node;
	}
	
	@Override
	protected boolean exec() {
		result = node.computeLocalSkyline();
		if(notify) addLocalSkyline(result);
		return true;
	}

	@Override
	public InputData[] getRawResult() 
	{
		return result;
	}

	@Override
	protected void setRawResult(InputData[] flv) {
		result = flv;
	}
}	

	
	private final class DirectoryNodeWorker extends RecursiveTask<InputData[]> 
	{
		private static final long serialVersionUID = 1L;
		private final LSDAbstractTree.DirectoryNode node;
		private final boolean notify; 
		DirectoryNodeWorker(boolean notify, final LSDAbstractTree.DirectoryNode node)
		{
			this.notify = notify;
			this.node = node;
		}
		@Override
		protected InputData[] compute() 
		{
			final ForkJoinTask<InputData[]> leftWorker 
				= (node.left instanceof LSDAbstractTree.DirectoryNode) 
				? new DirectoryNodeWorker(false, (LSDAbstractTree.DirectoryNode) node.left) 
				: new BucketNodeWorker(false, (LSDAbstractTree.BucketNode) node.left); 
			leftWorker.fork();
			final ForkJoinTask<InputData[]> rightWorker 
			= (node.right instanceof LSDAbstractTree.DirectoryNode) 
			? new DirectoryNodeWorker(false, (LSDAbstractTree.DirectoryNode) node.right) 
			: new BucketNodeWorker(false, (LSDAbstractTree.BucketNode) node.right); 
			
			InputData[] result = SBBase.combineLocalSkyline(scoringFunction, leftWorker.join(), rightWorker.invoke());
			if(notify) addLocalSkyline(result);
			return result;
		   }
	 }
	 
}
