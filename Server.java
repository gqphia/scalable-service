/**
 * Server:  This server class is multi purpose, it can act as the controller,
 * backend or the frontend.  It also contains the scaling logic for spawning
 * new slave vms depending on the volume of the user requests.
 * 
 */
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.RemoteException;
import java.rmi.registry.*;
import java.rmi.Naming;
import java.util.List;
import java.util.ArrayList;

import java.util.HashMap;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.LinkedBlockingQueue;

public class Server extends UnicastRemoteObject implements RemoteInterface
{

    /**
     * VM ID List.
     */
    public static ConcurrentLinkedDeque<Integer> frontEndList = new ConcurrentLinkedDeque<Integer>();
    public static ConcurrentLinkedDeque<Integer> appServerList = new ConcurrentLinkedDeque<Integer>();


    public static ConcurrentLinkedDeque<Integer> activeFrontEndList = new ConcurrentLinkedDeque<Integer>();
    public static ConcurrentLinkedDeque<Integer> activeBackEndList = new ConcurrentLinkedDeque<Integer>();

	public static final String CONTROLLER = "CONTROLLER";
    public static final String CACHENAME = "CACHE";

    public static Cloud.DatabaseOps cache = null;

    /**
     * Role IDs
     */
    public static final int MASTERID = 0;
    public static final int FRONTENDROLEID = 1;
    public static final int APPSERVERROLEID = 2;
    public static final int FAILURE = -1;

    public static String sCloudIPAddr =  "";
    public static String sCloudPortNumber= "";

   

    public static AtomicInteger requestCount = new AtomicInteger(0);

    /**
     * used by the slave VMs to have their ID
     */
    private static int currentRole;

    private static RemoteInterface controller;

    private static long startTime;
    private static long lastQueueCheckedTime = 0;

    private static long lastQueueSize =0;
    private static long  currentAppServers = 0;

    private static long scaleUpThreshold = 10;

    /**
     * Timeouts and other constants
     */
    private static final long bootUpTimeout = 3000;
    private static final int scaleTimeout  = 2000;
    private static final int shutDownTimeout  = 5750;
    private static final float LoadFactor= 1.7f;
    private static final int vmLimit = 10;
    private static final int dropThreshold = 5;
    private static final int minimumBackend = 2;

    /**
     * Times
     */
    private static long lastProcessedTime = 0;


    /**
     * Main request Queue: The FrontEnd VM(s) will add to the queue.
     */
    public static ConcurrentLinkedQueue<Cloud.FrontEndOps.Request> requestQueue
                            = new ConcurrentLinkedQueue<Cloud.FrontEndOps.Request>();


    /**
     * Constructor for server.
     */
    public Server()throws RemoteException
    {
        super();
    }


    /* Start RMI Methods Definitions.*/

    /**
     * Called by slave VMs to see if it is permitted to be shutdown.
     * @return  boolean value containing the permission 
     * @throws RemoteException
     */
    public synchronized boolean shutdownVM(int vmId) throws RemoteException
    {

    	/**
    	 * If it is the only remaning backend, it cannot be shutdown as
    	 * we need atleast one backend running for the service to be functional.
    	 */
    	if(activeBackEndList.size() < minimumBackend)
    		return false;

   		appServerList.remove(vmId); 
   		activeBackEndList.remove(vmId);
   		currentAppServers--;
   		return true;
    }


    /**
     * Gets the role of the VM.
     * @param vmID
     * @return role for the vm that calls this remote method.
     * @throws RemoteException
     */
    public int getRole(int vmID) throws RemoteException
    {
        if(frontEndList.contains(vmID))
        {
            return FRONTENDROLEID;
        }
        else if(appServerList.contains(vmID))
        {
            return APPSERVERROLEID;
        }

        return FAILURE;

    }


    /**
     * Invoked by the frontends to put request on to the queue.
     * @param r = Request Object
     */
    public void putRequest(Cloud.FrontEndOps.Request r) throws RemoteException
    {
        //Adds to the queue.
        try 
        {
            requestQueue.add(r);
        }
        catch(Exception e)
        {

        }
    }

    /**
     * Invoked by the BackEnds to get the request from the Queue.
     * @param
     * @throws Exception
     */
    public Cloud.FrontEndOps.Request getRequest() throws RemoteException
    {
        requestCount.getAndDecrement();    
        return requestQueue.poll();
    }

    /**
     * frontEndRunning(): Register the caller vm as a activefrontend with the 
     * controller
     * @throws RemoteException
     */
    public synchronized void frontEndRunning(int vmId) throws RemoteException
    {
        activeFrontEndList.add(vmId);

    }

    /**
     * appServerRunning(): Register the caller vm as an active appserver with the 
     * controller.
     * @throws RemoteException
     */
    public synchronized void appServerRunning(int vmId) throws RemoteException
    {
        activeBackEndList.add(vmId);
        currentAppServers++;
    }


    /* End of RMI Methods*/

    /** Start -Slave Local functions; */
    /**
     * Gets the controller instance to make rmi calls with.
     */
    public static void configureController()
    {
	    try 
	    {
	        controller = (RemoteInterface) Naming.lookup("//" + sCloudIPAddr + ":" + sCloudPortNumber + "/" + CONTROLLER);
	    }
	    catch(Exception e)
	    {
	        e.printStackTrace();
	    }
    }

     /* End- Slave Local methods*/



    /**
     * Main Method
     * @param args
     * @throws Exception
     */
	public static void main ( String args[] ) throws Exception
	{
        startTime = System.currentTimeMillis();
		if (args.length != 3) throw new Exception("Need 3 args: <cloud_ip> <cloud_port> <VM id>");

        // Set cloud parameters
        sCloudIPAddr = args[0];
        sCloudPortNumber = args[1];

        ServerLib SL = new ServerLib( args[0], Integer.parseInt(args[1]));

        int totalVMCount = 1;

		/*Added*/
		int thisVMId = Integer.parseInt(args[2]);
		Server s = new Server();
        boolean isController = isControllerInstance(args[2],s);

        if(isController)
        {


            //FrontEnd
            frontEndList.add(SL.startVM());
            totalVMCount++;

            //Backend
            appServerList.add(SL.startVM());
            totalVMCount++;


            new Cache(sCloudIPAddr,sCloudPortNumber);
           
            

            SL.register_frontend();

            int droppedReqCount  = 0;
            int counter=0;

            while( System.currentTimeMillis() - startTime  < bootUpTimeout )
            {
                
                Cloud.FrontEndOps.Request r = SL.getNextRequest();
				
				if(droppedReqCount == dropThreshold)
				{
					appServerList.add(SL.startVM());
					droppedReqCount = 0;
				}

				counter++;

                if( activeBackEndList.size() == 0)
                {
                	droppedReqCount++;
                	SL.drop(r);
                }
            	else
            	{
            		
                	requestQueue.add(r);
                	break;
            	}

            }

            
            
            

            // main loop
            while (true)
            {

         	    Cloud.FrontEndOps.Request r = SL.getNextRequest();
         	    
         	    if( requestQueue.size() / appServerList.size() > LoadFactor)
         	    {
         	    	
         	    	SL.drop(r);
         	    }
         	    else
         	    {
         	    	//drop
         	    	requestQueue.add(r);
         	    	
         	    }

         	    /* Start - Scaling logic */
				int totalSize = appServerList.size();
         		int queueSize = requestQueue.size();
         		int activeSize = activeBackEndList.size();
         		int difference =  queueSize - totalSize;
				

         		if(activeSize == totalSize  && difference > 0 ) 
         		{
         			

         			for(int index=0 ; index < difference ; index++)
         			{
         				if(appServerList.size() < vmLimit)
         				{
         					appServerList.add(SL.startVM());
         				}
         			}
         		}
                           
                /*End of Scaling Logic */
            }

        }
        else // Child VM
        {
            

            currentRole = controller.getRole(thisVMId);

            cache = (Cloud.DatabaseOps)Naming.lookup("//"+sCloudIPAddr+":"+sCloudPortNumber+"/"+CACHENAME);
            
            if(currentRole == FRONTENDROLEID)
            {

                controller.frontEndRunning(thisVMId);
                
                SL.register_frontend();

               
                // main loop
                while (true)
                {
                    Cloud.FrontEndOps.Request r = SL.getNextRequest();


                    if(r!=null)
                    {
                        try
                        {

                            controller.putRequest(r);
                        }
                        catch(Exception e)
                        {
               				         
                        }
                    }

                }
            }



            if(currentRole == APPSERVERROLEID)
            {
                controller.appServerRunning(thisVMId);
                

                while(true)
                {
                    try
                    {
                        Cloud.FrontEndOps.Request r = controller.getRequest();
                        if (r != null)
                        {
                            
                            SL.processRequest(r,cache);
                            lastProcessedTime = System.currentTimeMillis();
                            
                        }
                        
                    	if(System.currentTimeMillis() - lastProcessedTime >= shutDownTimeout)
                    	{	                        	
                        	if(controller.shutdownVM(thisVMId))
                        	{
                    
                        		UnicastRemoteObject.unexportObject(s,true);
                        		break;
                        		
                        	}
                        	
                    	}
                        
                    }
                    catch(Exception e)
                    {
                        
                    }

                    

                }

                SL.shutDown();

            }


        }
	}


    /**
     * The current VM tries to bind itself as the controller. If there is no
     * existing binding, it is the controller. Else, it is a slave. Hence it
     * registers itself as a slave in the cloud RMI registry.
     * @param vmID
     * @return true if registered as controller, false if registered
     */
    public static boolean isControllerInstance(String vmID, Server thisServer)
    {

        try
        {
            Naming.bind("//"+sCloudIPAddr+":"+sCloudPortNumber+"/"+CONTROLLER,thisServer);
            return true;
        }

        catch(Exception e)// Slave
        {
            configureController();
            
            try
            {
                Naming.bind("//"+sCloudIPAddr+":"+sCloudPortNumber+"/"+ vmID, thisServer );
            }
            catch(Exception e2)
            {
                e2.printStackTrace();
            }
        }

        return false;
    }


}

