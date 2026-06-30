// clase Topic
package broker;
//import java.awt.Event;
import pubsub.Event;
import pubsub.Subscriber;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.LinkedList;
import java.util.Set;
import java.util.Collection;
import java.util.HashSet;
import pubsub.Subscriber;


class Topic {
    private String name;
    private Queue<Event> eventQ;
    private Set<Subscriber> subs= new HashSet<Subscriber>();

    public Topic(String Top_name) {
        name= Top_name;
        eventQ = new LinkedList<Event>();
    }

    public String get_Name(){
        return name;
    }

    public boolean enqueue(Event ev){ //Se que es un poco lio esto pero asi el pubsub del broker 
                         // realiza el retunrn directamente
        return eventQ.add(ev);

    }

    public Event yumTopic(){
       return eventQ.poll(); 
    }

    public  void addSub(Subscriber sub){
        subs.add(sub);
    }

    public Collection<Subscriber> getSubs(){
        return subs;
    }

}
 