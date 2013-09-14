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
 * ANY EXPRESS OR IMPLIED WARRinputIteratorES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRinputIteratorES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.math3.linear.BlockRealMatrix;
import org.apache.commons.math3.random.CorrelatedRandomVectorGenerator;
import org.apache.commons.math3.random.GaussianRandomGenerator;
import org.apache.commons.math3.random.MersenneTwister;

import skylinebreaker.LSDAbstractTree;
import skylinebreaker.LSDPruningTree;
import skylinebreaker.LSDSharedPruningTree;
import skylinebreaker.LSDSimpleTree;
import skylinebreaker.SBFork;
import skylinebreaker.SBQuick;
import skylinebreaker.SBQuickCNN;
import skylinebreaker.SBSingle;

import compat.BasicScoring;
import compat.InputData;
import compat.ScoringFunction;

public class Main {

	static class ResultInfo
	{
		long secs;
		InputData[] skyline;
		ResultInfo(long secs, InputData[] skyline) 
		{
			this.secs = secs;
			this.skyline = skyline;
		}
	}
	
	
    public static LSDAbstractTree fillLSDTree(LSDAbstractTree.LSDAbstractTreeFactory factory, Iterator<InputData> inputIterator, ScoringFunction scoringFunction) throws IllegalStateException
    {
    	LSDAbstractTree tree = factory.get(scoringFunction, (InputData) inputIterator.next());
    	while(inputIterator.hasNext()) tree.add((InputData)inputIterator.next());
    	return tree;
    }
	
	public static ResultInfo runSBFork(List<InputData> input,ScoringFunction scoringFunction, int numCPUs) throws InterruptedException, IllegalStateException {

        Iterator<InputData> inputIterator = input.iterator();
        StopWatch sw = new StopWatch();
        sw.start();
		InputData[] mySkyline = new SBFork(numCPUs, new LSDSimpleTree.LSDSimpleTreeFactory(), inputIterator, scoringFunction).compute();
	    sw.stop();
	    return new ResultInfo(sw.getNanoTime(), mySkyline);
	}
	public static ResultInfo runSBForkP(List<InputData> input,ScoringFunction scoringFunction , int numCPUs) throws InterruptedException, IllegalStateException {

        Iterator<InputData> inputIterator = input.iterator();
        StopWatch sw = new StopWatch();
        sw.start();
		InputData[] mySkyline = new SBFork(numCPUs, new LSDPruningTree.LSDPruningTreeFactory(), inputIterator, scoringFunction).compute();
		sw.stop();
	    return new ResultInfo(sw.getNanoTime(), mySkyline);
	}
	public static ResultInfo runSBForkSP(List<InputData> input,ScoringFunction scoringFunction, int numCPUs) throws InterruptedException, IllegalStateException {

        Iterator<InputData> inputIterator = input.iterator();
        StopWatch sw = new StopWatch();
        sw.start();
		InputData[] mySkyline = new SBFork(numCPUs, new LSDSharedPruningTree.LSDSharedPruningTreeFactory(), inputIterator, scoringFunction).compute();
	    sw.stop();
	    return new ResultInfo(sw.getNanoTime(), mySkyline);
	}
	public static ResultInfo runSBSingle(List<InputData> input,ScoringFunction scoringFunction) throws InterruptedException, IllegalStateException {
        Iterator<InputData> inputIterator = input.iterator();
        StopWatch sw = new StopWatch();
        sw.start();
		InputData[] mySkyline = new SBSingle(fillLSDTree(new LSDSimpleTree.LSDSimpleTreeFactory(), inputIterator, scoringFunction)).computeSkyline(scoringFunction);
	    sw.stop();
	    return new ResultInfo(sw.getNanoTime(), mySkyline);
	}
	public static ResultInfo runSBQuick(List<InputData> input,ScoringFunction scoringFunction, int numCPUs) throws InterruptedException, IllegalStateException 
	{
        Iterator<InputData> inputIterator = input.iterator();
        StopWatch sw = new StopWatch();
        sw.start();
        InputData[] mySkyline = SBQuick.evaluate(numCPUs, new LSDSimpleTree.LSDSimpleTreeFactory(), inputIterator, scoringFunction);
	    sw.stop();
	    return new ResultInfo(sw.getNanoTime(), mySkyline);
	}

	

	public static ResultInfo runSBSingleP(List<InputData> input,ScoringFunction scoringFunction) throws InterruptedException, IllegalStateException {
        Iterator<InputData> inputIterator = input.iterator();
        StopWatch sw = new StopWatch();
        sw.start();
		InputData[] mySkyline = new SBSingle(fillLSDTree(new LSDPruningTree.LSDPruningTreeFactory(), inputIterator, scoringFunction)).computeSkyline(scoringFunction);
	    sw.stop();
	    return new ResultInfo(sw.getNanoTime(), mySkyline);
	}

	public static ResultInfo runSBQuickSP(List<InputData> input,ScoringFunction scoringFunction, int numCPUs) throws InterruptedException, IllegalStateException 
	{
        Iterator<InputData> inputIterator = input.iterator();
        StopWatch sw = new StopWatch();
        sw.start();
        InputData[] mySkyline = SBQuick.evaluate(numCPUs, new LSDSharedPruningTree.LSDSharedPruningTreeFactory(), inputIterator, scoringFunction);
	    sw.stop();
	    return new ResultInfo(sw.getNanoTime(), mySkyline);
	}
	public static ResultInfo runSBQuickP(List<InputData> input,ScoringFunction scoringFunction, int numCPUs) throws InterruptedException, IllegalStateException 
	{
        Iterator<InputData> inputIterator = input.iterator();
        StopWatch sw = new StopWatch();
        sw.start();
        InputData[] mySkyline = SBQuick.evaluate(numCPUs, new LSDPruningTree.LSDPruningTreeFactory(), inputIterator, scoringFunction);
	    sw.stop();
	    return new ResultInfo(sw.getNanoTime(), mySkyline);
	}
	
	public static ResultInfo runSBQuickCNN(List<InputData> input,ScoringFunction scoringFunction, int numCPUs) throws InterruptedException, IllegalStateException 
	{
        Iterator<InputData> inputIterator = input.iterator();
        StopWatch sw = new StopWatch();
        sw.start();
        InputData[] mySkyline = SBQuick.evaluate(numCPUs, new LSDSimpleTree.LSDSimpleTreeFactory(), inputIterator, scoringFunction);
	    sw.stop();
	    return new ResultInfo(sw.getNanoTime(), mySkyline);
	}
	public static ResultInfo runSBQuickPNNP(List<InputData> input,ScoringFunction scoringFunction, int numCPUs) throws InterruptedException, IllegalStateException 
	{
        Iterator<InputData> inputIterator = input.iterator();
        StopWatch sw = new StopWatch();
        sw.start();
        InputData[] mySkyline = SBQuickCNN.evaluate(numCPUs, new LSDPruningTree.LSDPruningTreeFactory(), inputIterator, scoringFunction);
	    sw.stop();
	    return new ResultInfo(sw.getNanoTime(), mySkyline);
	}
    
    
	
	public static void main(String[] args) throws InterruptedException, IllegalStateException {
		
		final double[] means = new double[] { 10, 10};
		final int setsize = 100000;
		final int loops = 5;
		
		CorrelatedRandomVectorGenerator generator = 
				 new CorrelatedRandomVectorGenerator(
						 means,
                         new BlockRealMatrix( new double[][] { {10, 5}, {5,  10}  } ),
                         0.01,
                         new GaussianRandomGenerator(new MersenneTwister()));
		 
		List<InputData> list = new LinkedList<InputData>();
		final int[] dimensions = new int[means.length]; 
		for(int i = 0; i < dimensions.length; ++i) dimensions[i] = 0;
		
		System.out.println("Generating set with size=" + setsize);
		 outer:
		 while(list.size() < setsize)
		 { 
			
			final int[] ivec = new int[means.length];
			{
				double[] vec = generator.nextVector();
			 	for(int j = 0; j < vec.length; ++j)
			 	{
			 		ivec[j] = (int) (vec[j] * 10);
			 		if(ivec[j] < 0) continue outer;
			 		
			 	}
			}
		 	for(int j = 0; j < ivec.length; ++j)
		 		if(ivec[j] > dimensions[j]) dimensions[j] = ivec[j];
		 	list.add(new InputData(ivec));
			
		 	//System.out.println(Arrays.toString(ivec));
		}
		
		class ProjectiveScoring implements BasicScoring
		{
			final int pos;
			final int maximum;
			ProjectiveScoring(int i)
			{
				this.pos = i;
				this.maximum = dimensions[i];
			}
			@Override
			public int evaluate(InputData x) {
				return x.get(pos);
			}

			@Override
			public int getMaximum() {
				return maximum;
			}
		}
		
		
		BasicScoring[] scorings = new BasicScoring[dimensions.length];
		for(int i = 0; i < scorings.length; ++i)
			scorings[i] = new ProjectiveScoring(i);
		
		ScoringFunction f = new ScoringFunction(scorings);
		
		ResultInfo ri = runSBSingle(list, f);
		System.out.println("Skyline of size=" + ri.skyline.length);
		for(InputData s : ri.skyline)
		{
			System.out.println(s);
		}
		
		
		{
			long time = 0;
			for(int loop = 0; loop < loops; ++loop)
			{
				ResultInfo ris = runSBSingle(list, f);
				System.out.println("SBSingle, time " + ris.secs);
				time += ris.secs;
				assert ri.skyline.length == ris.skyline.length;
				for(InputData s : ri.skyline)
				{
					boolean found = false;
					for(InputData t : ris.skyline)
					{
						if(t.equals(s)) found = true;
					}
					assert found == true;
				}
				
				
			}
			System.out.println("SBSingle, arith. time " + (time/loops));
		}
		
		{
			long time = 0;
			for(int loop = 0; loop < loops; ++loop)
			{
				ResultInfo ris = runSBFork(list, f, Runtime.getRuntime().availableProcessors());
				System.out.println("SBFork, time " + ris.secs);
				time += ris.secs;
				assert ri.skyline.length == ris.skyline.length;
				for(InputData s : ri.skyline)
				{
					boolean found = false;
					for(InputData t : ris.skyline)
					{
						if(t.equals(s)) found = true;
					}
					assert found == true;
				}
				
				
			}
			System.out.println("SBFork, arith. time " + (time/loops));
		}
		{
			long time = 0;
			for(int loop = 0; loop < loops; ++loop)
			{
				ResultInfo ris = runSBQuick(list, f, Runtime.getRuntime().availableProcessors());
				System.out.println("SBQuick, time " + ris.secs);
				time += ris.secs;
				assert ri.skyline.length == ris.skyline.length;
				for(InputData s : ri.skyline)
				{
					boolean found = false;
					for(InputData t : ris.skyline)
					{
						if(t.equals(s)) found = true;
					}
					assert found == true;
				}
				
				
			}
			System.out.println("SBQuick, arith. time " + (time/loops));
		}
		
		
		
	}

	
	
	
	
}
