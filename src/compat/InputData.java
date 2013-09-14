package compat;
import java.util.Arrays;


public class InputData {

	public int get(int i) {
		return v[i];
	}
    private final int[] v;
    
	public InputData(int[] v) 
	{
		this.v = v;
	}
	@Override
	public String toString() {
		return Arrays.toString(v);
	}
	
}
