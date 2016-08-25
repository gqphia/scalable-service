/**
 * Interface for communication between Server processes
 */
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface RemoteInterface extends Remote
{
    public boolean shutdownVM(int vmID) throws RemoteException;
    public int getRole(int vmID) throws RemoteException;
    public void putRequest(Cloud.FrontEndOps.Request r) throws RemoteException;
    public Cloud.FrontEndOps.Request getRequest() throws RemoteException;
    public void frontEndRunning(int vmId) throws RemoteException;
    public void appServerRunning(int vmId) throws RemoteException;

}
