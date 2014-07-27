package ap.v2;

import java.io.PrintStream;

public class Timer
{
    private long startTime;
    private long endTime;
    
    public Timer() 
    {   
    	reset(); 
    }

    public void start() 
    {
        startTime = System.currentTimeMillis();
    } 
    
    public void end() 
    {
        endTime = System.currentTimeMillis();
	}
    
    public long duration() 
    {
        return (endTime - startTime);
    }
    
    public void printDuration( PrintStream out ) 
    {
        long elapsedTimeInSecond = duration() / 1000;
        long remainderInMillis = duration() % 1000;
        out.println("\nTotal execution time:"
        			+ elapsedTimeInSecond + "."
        			+ remainderInMillis
        			+ " seconds");
    }
    
    public void reset() 
    {
        startTime = 0;
        endTime = 0;
    }
}