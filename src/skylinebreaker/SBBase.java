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


public abstract class SBBase {


	public static final class NearestNeighborOfZero
	{
		public InputData neighbor = null;
		public int norm = Integer.MAX_VALUE;
		public int threadCount = 0;
	}
	
	 public static InputData[] combineLocalSkyline(ScoringFunction scoringFunction, final InputData[] a, final InputData[] b) 
		{
			int areserved = a.length;
			int breserved = b.length;
			
			for(int i = 0; i < a.length && i < areserved; ++i)
			{
				if(b.length == 0) break;
				aLoop:
				for(int j = 0; j < b.length && j < breserved; ++j)
				{
					bLoop:
					switch(scoringFunction.evaluate(a[i]).compare(scoringFunction.evaluate(b[j]))) {
					case FeatureVector.LESS: 
						a[i--] = a[--areserved];
						break aLoop;
					case FeatureVector.GREATER: 
						b[j--] = b[--breserved];
						break bLoop;
					}
				}
			}
			final InputData[] c = new InputData[areserved + breserved];
			for(int i = 0; i < areserved; ++i) c[i] = a[i];
			for(int i = 0; i < breserved; ++i) c[i+areserved] = b[i];
			return c;
		}
}
