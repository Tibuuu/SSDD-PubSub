// Clase que define un evento
// NO MODIFICAR
package pubsub;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Event implements Serializable {
    public static final long serialVersionUID=1234567890L;
    String topic;
    HashMap <String,Object> content;
    public Event(String t, Map <String,Object> c) {
        topic = t;
        content = new HashMap<>(c);
    }
    public String getTopic() {
        return topic;
    }
    public Map <String,Object> getContent() {
        return content;
    }

    @Override
    public String toString() {
        return "topic = <" + topic + "> content = <" + content + ">";
    }
}
