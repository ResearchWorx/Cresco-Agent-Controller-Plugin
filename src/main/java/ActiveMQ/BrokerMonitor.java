package ActiveMQ;

class BrokerMonitor implements Runnable 
{
	  private String agentPath;
	  private boolean MonitorActive;
	  public BrokerMonitor(String agentPath)
	  {
	    	this.agentPath = agentPath;
	    	this.MonitorActive = true;
	  }
	  public void shutdown()
	  {
			
	  }
	  public void run() 
	  {
		  try
		  {
	    		while(MonitorActive)
	    		{
	    			Thread.sleep(1000);
	    		}
		  }
		  catch(Exception ex)
		  {
	    		
		  }
	  }
}