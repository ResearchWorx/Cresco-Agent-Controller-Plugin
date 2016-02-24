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
			MonitorActive = false;
	  }
	  public void run() 
	  {
		  try
		  {
	    		while(MonitorActive)
	    		{
	    			System.out.println("Monitoring thread for : " + agentPath);
	    			Thread.sleep(5000);
	    		}
		  }
		  catch(Exception ex)
		  {
	    		
		  }
	  }
}