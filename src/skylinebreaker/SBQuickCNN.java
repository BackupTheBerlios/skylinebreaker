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

import skylinebreaker.LSDAbstractTree.LSDAbstractTreeFactory;
import skylinebreaker.SBBase.NearestNeighborOfZero;

import compat.InputData;
import compat.ScoringFunction;

public final class SBQuickCNN {
	
	public static InputData[] evaluate(final int processes, final LSDAbstractTreeFactory treeFactory, final Iterator<InputData> inputIterator, final ScoringFunction scoringFunction) throws InterruptedException
	{
		SBSingleCNN.nnzero = new NearestNeighborOfZero();
		final Queue<Integer> notification = new LinkedList<Integer>();
		
		final class LSDWorker extends Thread
		{
			
			InputData[] data = null;
			private final InputData init_value;
			final int number;
			public LSDWorker(final int number, final InputData init_value)
			{
				this.number = number;
				this.init_value = init_value;
			}
			
	    	@Override
	    	public void run() 
	    	{
	    		try 
	    		{
		    		final LSDAbstractTree tree = treeFactory.get(scoringFunction, init_value);
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
		    		
						data = new SBSingleCNN(tree, processes).computeSkyline(scoringFunction);
				}
	    		catch (InterruptedException e1) 
	    		{
					e1.printStackTrace();
	    		}
	    		synchronized(notification) { notification.add(number); notification.notifyAll(); }
	    	}
		}
		
		//final int processes = Runtime.getRuntime().availableProcessors();
		final LSDWorker[] threads = new LSDWorker[processes];
		for(int i = 0; i < processes; ++i) threads[i] = new LSDWorker(i, (InputData) inputIterator.next());
		for(int i = 0; i < processes; ++i) threads[i].start();
		final SkylineMergeQueue skylineMergeQueue = new SkylineMergeQueue(scoringFunction, processes);
		int finishedThreads = 0;
		while(finishedThreads != threads.length)
		{
			int threadNum;
			synchronized(notification) 
			{ 
				while(notification.isEmpty()) notification.wait(); 
				threadNum = notification.poll();
			}
			++finishedThreads;
			final LSDWorker thread = threads[threadNum];
			thread.join();
			final InputData[] localSkyline = thread.data;
			if(localSkyline != null) skylineMergeQueue.add(localSkyline);
		}
		skylineMergeQueue.stop();
		
		for(SkylineMergeQueue.SkylineMergeWorker worker : skylineMergeQueue.threads)
		{
			worker.interrupt();
			worker.join();
		}
		assert skylineMergeQueue.computedLocalSkylines.size() == 1;
		return skylineMergeQueue.computedLocalSkylines.poll();
	}

	final static class SkylineMergeQueue
	{
		final Queue<InputData[]> computedLocalSkylines = new LinkedList<InputData[]>();
		final SkylineMergeWorker[] threads;
		private boolean running = true;
		private final ScoringFunction scoringFunction;
		public SkylineMergeQueue(ScoringFunction scoringFunction, final int maxThreads)
		{
			this.scoringFunction = scoringFunction;
			this.threads = new SkylineMergeWorker[Math.max(maxThreads/2, 1)];
			for(int i = 0; i < threads.length; ++i)
			{
				threads[i] = new SkylineMergeWorker();
				threads[i].start();
			}

		}
		public void stop()
		{
			running = false;
		}
		public void add(final InputData[] node)
		{
		       synchronized(computedLocalSkylines) {
		    	   computedLocalSkylines.add(node);
		    	   computedLocalSkylines.notify();
		        }
		}
		
		 class SkylineMergeWorker extends Thread 
		 {
		    	@Override
		    	public void run() 
		    	{
		    		try
		    		{
		    			while(running || !computedLocalSkylines.isEmpty())
		    			{
		    				InputData[] a;
		    				InputData[] b;
			    			synchronized(computedLocalSkylines) 
			    			{
					    		while(computedLocalSkylines.size() < 2) 
					    			computedLocalSkylines.wait();
					    		a = computedLocalSkylines.poll();
					    		b = computedLocalSkylines.poll();
			    			}
			    			
			    			InputData[] c = SBBase.combineLocalSkyline(scoringFunction, a, b);
			    			add(c);
		    			}
		    		}
		            catch (InterruptedException ignored)
		            {
		            }
		    	}
		    }
		
		
		
	}
	
	
}
