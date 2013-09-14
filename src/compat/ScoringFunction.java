package compat;

public class ScoringFunction {

	public ScoringFunction(BasicScoring[] scorings)
	{
		this.scorings = scorings;
		{
	        int _sum = 0;
	        for(BasicScoring scoring : scorings) _sum += scoring.getMaximum();
	        combinedDimensionLength = _sum;
		}
	}
	public final BasicScoring[] scorings; 
	public final int combinedDimensionLength;

	public BasicScoring getProjection(int i) {
		return scorings[i];
	}
	public int getDimensionality() {
		return scorings.length;
	}
	
	public FeatureVector evaluate(InputData data)
	{
		int[] v = new int[scorings.length];
		for(int i = 0; i < v.length; ++i)
			v[i] = scorings[i].evaluate(data);
		return new FeatureVector(v, this);
	}
	
}
