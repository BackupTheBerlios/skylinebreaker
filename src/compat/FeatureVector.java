package compat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;


public class FeatureVector {

    public static final int SUBSTITUTABLE = 0;
    public static final int GREATER = 1;
    public static final int LESS = -1;
    public static final int UNRANKED = 2;
	
    private final int[] v;
    public final int overallLevel;
    private final ScoringFunction scoringFunction;
	public FeatureVector(int[] v, ScoringFunction scoringFunction) 
	{
		this.scoringFunction = scoringFunction;
		this.v = v;
		{
			int overall = 0;
			for(int l : v)
				overall += l;
			overallLevel = overall;
		}
	}


	public int compare(FeatureVector o) 
	{
		int result = SUBSTITUTABLE;
		for(int i = 0; i < v.length; ++i) 
		{
		    if(v[i] < o.v[i]) 
		    {
		    	if(result == LESS) return UNRANKED;
		    	result = GREATER;
		    } 
		    else if(this.v[i] > o.v[i]) 
		    {
		    	if(result == GREATER) return UNRANKED;
		    	result = LESS;
		    }
		}
		return result;
	}


	public int getLevel(int i) {
		return v[i];
	}

	@Override
	public String toString() {
		return Arrays.toString(v);
	}
	
	public double getPruningLevel()
	{
        if(v.length == 1) return v[0]+1; // ScoringFunction is a preorder
        if(overallLevel == 0) return 1;
        
		if(overallLevel == scoringFunction.combinedDimensionLength) return scoringFunction.combinedDimensionLength+1;
        
        List<Integer> vector_difference = new ArrayList<Integer>();
        for(int i = 0; i < v.length; ++i)
        {   
            if(v[i] == 0) continue;
            vector_difference.add(scoringFunction.getProjection(i).getMaximum() - v[i]);
        }
        return scoringFunction.combinedDimensionLength - findMinimum(vector_difference);
	}
	private static int findMinimum(Collection<Integer> array)
    {   
        int min = Integer.MAX_VALUE;
        for(int i : array)
            if(i < min) min = i;
        return min;
    }
	
	
}
