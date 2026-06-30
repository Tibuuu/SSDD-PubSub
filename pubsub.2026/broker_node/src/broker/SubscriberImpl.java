// Clase que implementa la interfaz remota Subscriber
package broker;
import java.nio.file.Paths;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.rmi.RemoteException;
import java.rmi.NoSuchObjectException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Collection;   
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Flow;
import pubsub.Subscriber;
import pubsub.SubscriberCallback;
import pubsub.Event;



class SubscriberImpl extends UnicastRemoteObject implements Subscriber  {
    public static final long serialVersionUID=1234567890L;
    boolean finished=false;
    UUID subUUID; // para facilitar depuración
    PubSubImpl ps; // para acceder a funcionalidad del servicio general
    // para notificar al subscriptor de creación y destrucción de temas
    transient SubscriberCallback scbk; 
    private Queue<Event> eventSub;
    private Set<String> sub_Topics= new HashSet<String>();

    public SubscriberImpl(PubSubImpl p, SubscriberCallback s) throws RemoteException {
        scbk=s;
        subUUID = UUID.randomUUID();
	    ps=p;
        eventSub= new LinkedList<Event>();
    }
    public UUID getUUID() throws RemoteException {
        finish(finished);
        return subUUID;
    }
    public int subscribe(String topic, boolean glob) throws RemoteException {
        finish(finished);
        if(glob){
            PathMatcher match= FileSystems.getDefault().getPathMatcher("glob:"+topic);
            int res=0;
            for(String p : ps.topicList()){
                if(match.matches(Paths.get(p)))
                    res+=subscribe(p,false);
            }
            return res;
        }else{
        Topic top= ps.getTopic(topic);
        if(top==null)
            return 0;
        if(sub_Topics.contains(topic))
            return 0;
        sub_Topics.add(topic);
        top.addSub(this);
        return 1;
        }
    }

    public void addEvent(Event ev)throws RemoteException  {
        finish(finished);
        eventSub.add(ev);
    }

    
    public Event getEvent() throws RemoteException {
        finish(finished);
        return eventSub.poll();
    }
    public Collection<String> topicListBySubscriber() throws RemoteException {
        finish(finished);
        return sub_Topics;
    }
    public boolean unsubscribe(String topic) throws RemoteException {
        finish(finished);
        Topic top= ps.getTopic(topic);
        if(top==null|| !sub_Topics.contains(topic))
            return false;
        sub_Topics.remove(topic);
        top.getSubs().remove(this);

        return true;
    }
    public void exit() throws RemoteException {
        finished=true;
        for(String s : new LinkedList<>(sub_Topics)){
            unsubscribe(s);
        }
        ps.removeSub(this);
        
        //Esto es to raro pero es la unica manera de hacer la excepcion de noSUch object
        UnicastRemoteObject.unexportObject(this, true);
        

    }
    public void notifNewTopic(String topic) throws RemoteException{
        finish(finished);
        if(scbk!=null)
            scbk.topicAdded(topic);
    }

    public void notifRemovedTopic(String topic) throws RemoteException{
        finish(finished);
        if(scbk!=null)
            scbk.topicRemoved(topic);
    }
    private void  finish(boolean finished)throws NoSuchObjectException{
        if(finished)
            throw new NoSuchObjectException("this subscriber has already exited");
    }
}
