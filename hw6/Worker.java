import java.io.IOException;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Random;
import java.net.Socket;
import java.lang.Thread;


/** worker thread, generate rate changes, sending update(terminate) msg to processes 
*   @author yuemin
*/
public class Worker implements Runnable{
    
    public ZooKeeper zk;
    //public Watcher wh;
    public String root;
    public int operation_num;
    public Clock workerClock;//the local logical clock, not the real clock.    
    
    public void run(){
        
        //proceed with operations
        for(int j=0; j<operation_num+1; j++){
            //random generate
            Random rand = new Random();
            // rand%161 --> [0, 160], -80 --> [-80, 80]
            int deltaBuy = rand.nextInt() % 161 - 80;
            int deltaSell = rand.nextInt() % 161 - 80;
            
            //random update interval (0,1000] milliseconds
            long interval = rand.nextInt() % 1000 + 1; 
            Thead.sleep(interval);

            //String timestamp = workerClock.counter + 0.1*processID;//vetor clock counter.0, counter.1, conter.2
            String newZnode = root+"/"+"update";
            String update_value = deltaSell+","+deltaBuy;
            if(j==operation_num){//last update is the end message.
                update_value = "updateEnd";
            }
            if(!zk.exits(newznode, false)){//no such node returns null
                zk.create(newZnode, update_value.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }else{
                zk.setData(newZnoed, update_value.getBytes(), -1);//match with any version
            }
            workerClock.increaseStamp();//Lamport requirement

        
        }
    }

}
