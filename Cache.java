/**
 * The cache for the scalable service. Passes transactions and set requests to the original 
 * database. Updates the cache hashmap accordingly.
 */
import java.util.concurrent.ConcurrentHashMap;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.RemoteException;
import java.rmi.registry.*;
import java.rmi.Naming;

public class Cache  extends UnicastRemoteObject implements Cloud.DatabaseOps 
{
	public ConcurrentHashMap<String,String> cacheMap;
	public static final String CACHENAME = "CACHE";
	public static Cloud.DatabaseOps database;
	public static ServerLib SL;
	public String sCloudIP;
	public String sCloudPort;

	/**
	 * Constructor 
	 */
	public Cache(String cloudIP,String cloudPort) throws RemoteException
	{
		cacheMap = new ConcurrentHashMap<String,String>();
		database = null;
		SL = null;
		bindCache(cloudIP,cloudPort);
		sCloudIP = cloudIP;
		sCloudPort=cloudPort;

	}

	/**
	 * Configure Cache
	 */
	public void bindCache(String cloudIP, String cloudPort)
	{

        try
        {
            Naming.bind("//"+cloudIP+":"+cloudPort+"/"+CACHENAME,this);
            
            
        }

        catch(Exception e)// Slave
        {
        	e.printStackTrace();
        }

        SL = new ServerLib(cloudIP,Integer.parseInt(cloudPort));
        database = SL.getDB();

	}

	/**
	 * Sets the value 
	 * @param  key             
	 * @param  val             
	 * @param  auth            
	 * @return result of set operation.                
	 * @throws RemoteException 
	 */
	@Override
	public boolean set(String key, String val, String auth) throws RemoteException
	{
		if(database.set(key,val,auth)==true)
		{
			cacheMap.put(key,val);
			return true;
		}

		

		return false;
	}

	/**
	 * Gets the value from the cache. If the value is not present,
	 * @param  key             
	 * @return get : value for that key.                
	 * @throws RemoteException 
	 */
	@Override
	public String get(String key) throws RemoteException
	{
	
		String val = cacheMap.get(key);

		if(val != null)
		{
		
			return val;
		}

		val = database.get(key);

		cacheMap.put(key,val);
		
		return val;
	}

	/**
	 * Passes the transaction to the  database.Updates the quantity and price in
	 * the cache if the transaction succeeds.
	 * @param  item            
	 * @param  price           
	 * @param  qty             
	 * @return transaction success/failure               
	 * @throws RemoteException 
	 */
	@Override
	public boolean transaction(String item,float price,int qty) throws RemoteException
	{
		
		if(database.transaction(item,price,qty)==true)
		{
			String itemQty = item+"_qty";
			String itemPrice = item+"_price";


			if(cacheMap.get(itemQty) != null)
			{
				int diff = Integer.parseInt(cacheMap.get(itemQty)) - qty;
				cacheMap.put(itemQty,Integer.toString(diff));
			}

			cacheMap.put(itemPrice,Float.toString(price));
			
			return true;
		}

		return false;
	}
}	
