// Servidor que implementa la interfaz remota PubSub
package broker;
import java.rmi.RemoteException;
import java.rmi.NoSuchObjectException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.lang.Object;
import java.util.HashMap;
import java.util.Map;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import pubsub.Event;
import pubsub.PubSub;
import pubsub.Subscriber;
import pubsub.SubscriberCallback;

class PubSubImpl extends UnicastRemoteObject implements PubSub  {
    public static final long serialVersionUID=1234567890L;
    Map<String, Topic> topics= new HashMap<String, Topic>();

    public PubSubImpl() throws RemoteException {
    }
    public int getVersion() throws RemoteException { // ya programada
        return version;
    }
    public synchronized boolean createTopic(String topic) throws RemoteException {
        if(topics.containsKey(topic))
            return false;
        Topic top = new Topic(topic);
        topics.put(topic, top);
        return true;
    }
    public synchronized Collection<String> topicList() throws RemoteException {
        Iterator<String> list =topics.keySet().iterator();
        Collection <String> res = new LinkedList<String>();
        while(list.hasNext()){
            res.add(list.next());
        }
        return res;
    }
    public synchronized boolean publish(Event ev) throws RemoteException {
            Topic top= topics.get(ev.getTopic());
            if(top!=null)
                return top.enqueue(ev);
            return false;
    }
    public synchronized Event consumeEvent(String topic) throws RemoteException {
            if(!topics.containsKey(topic))
                throw new NoSuchObjectException("consumeEvent: no such topic");
        Topic top= topics.get(topic);
        return top.yumTopic();

    }
    public synchronized Subscriber initSubscriber(SubscriberCallback c) throws RemoteException {
        return null;
    }
    public synchronized Collection<Subscriber> subscriberList() throws RemoteException {
        return null;
    }
    public synchronized Collection<Subscriber> subscriberListByTopic(String topic) throws RemoteException {
        return null;
    }
    public synchronized boolean deleteTopic(String topic) throws RemoteException {
       return false;
    }
    static public void main (String args[])  {
        if (args.length!=1) {
            System.err.println("Usage: PubSubImpl registryPortNumber");
            return;
        }
        try {
            PubSub ps = new PubSubImpl();
            Server.init(ps, args[0]);
        }
        catch (Exception e) {
            System.err.println("PubSubImpl exception: " + e.toString());
            System.exit(1);
        }
    }
}
