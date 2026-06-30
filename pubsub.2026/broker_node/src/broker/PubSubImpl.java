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
import java.util.concurrent.Flow;
import pubsub.Event;
import pubsub.PubSub;
import pubsub.Subscriber;
import pubsub.SubscriberCallback;
import java.util.UUID;

class PubSubImpl extends UnicastRemoteObject implements PubSub  {
    public static final long serialVersionUID=1234567890L;
    Map<String, Topic> topics= new HashMap<String, Topic>();
    Map<UUID, SubscriberImpl> subs= new HashMap<UUID, SubscriberImpl>();

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
        
        Iterator<UUID> subList= subs.keySet().iterator();
        //Collection <UUID> res = new LinkedList<UUID>();
        while(subList.hasNext()){
            try{
            subs.get(subList.next()).notifNewTopic(topic);
            } catch(Exception e){
                System.err.println("createTopic: notif failed");
            }
        }
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
            if(top==null)
                return false;
            top.enqueue(ev);
            Iterator<Subscriber> subIt= subscriberListByTopic(ev.getTopic()).iterator();
            while(subIt.hasNext()){
                SubscriberImpl sub= (SubscriberImpl) subIt.next();
                sub.addEvent(ev);
            }
            return true;

    }

    
    public synchronized Event consumeEvent(String topic) throws RemoteException {
            if(!topics.containsKey(topic))
                throw new NoSuchObjectException("consumeEvent: no such topic");
        Topic top= topics.get(topic);
        return top.yumTopic();

    }
    public synchronized Subscriber initSubscriber(SubscriberCallback c) throws RemoteException {
        SubscriberImpl newSub = new SubscriberImpl(this, c);
        subs.put(newSub.getUUID(),newSub);
        return newSub;
    }

    public Topic getTopic(String Top_name){
        return topics.get(Top_name);
    }

    public synchronized Collection<Subscriber> subscriberList() throws RemoteException {
        return new LinkedList<Subscriber>(subs.values());
    }
    public synchronized Collection<Subscriber> subscriberListByTopic(String topic) throws RemoteException {
        Topic top = topics.get(topic);
        if(top==null)
            return null;
        return top.getSubs();
    }
    public synchronized void removeSub(SubscriberImpl sub){
        subs.remove(sub.subUUID);
    }
    public synchronized boolean deleteTopic(String topic) throws RemoteException {
        Topic top= topics.get(topic);
        if(top==null)
            return false;
        for(SubscriberImpl sub : subs.values()){
            try {
                sub.notifRemovedTopic(topic);
            } catch (Exception e) {
                System.err.println("deleteTopic: not failed");
            }
        }
        for(Subscriber s : new LinkedList<>(top.getSubs())){
            ((SubscriberImpl) s).unsubscribe(topic);
        }
        top.getSubs().clear();
        topics.remove(topic);
        return true;
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
