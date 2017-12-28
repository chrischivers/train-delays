import net.ser1.stomp.Listener;
import java.util.Map;

public class MyListener implements Listener {

    @Override
    public void message(Map header, String body) {
        System.out.println("| Got header: " + header);
        System.out.println("| Got body: " + body);
    }
}