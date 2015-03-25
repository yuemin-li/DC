import java.io.IOException;
import java.util.Random;
import java.lang.Thread;
import java.lang.InterruptedException;
import java.lang.Math;
import java.util.List;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;


/** worker thread, generate rate changes, sending update(terminate) msg to processes
*   sharing the same zk instance with TotalOrderZK class. 
*   @author yuemin
*/
public class Worker implements Runnable{
    
    public ZooKeeper zk;
    /*private Watcher wh = new Watcher() {
        public void process (WatchedEvent evet) {
            //do nothing
        }
    };
    public String address;
    */
    public String processID;
    public String root;
    public int operation_num;
    public Clock workerClock;//the local logical clock, not the real clock.    

    /*private static void createZKInstance(String address) {
    try {
            zk = new ZooKeeper(address, 10000, this.wh);
        } catch (IOException e) {
            System.out.println("Cannot create ZooKeeper Instance");
            System.out.println("Got an exception:" + e.getMessage());
        }
    }
    */

    public void run(){
        try{
        //proceed with operations
        for(int j=0; j<operation_num+1; j++){
            //random generate
            Random rand = new Random();
            // rand%161 --> [0, 160], -80 --> [-80, 80]
            int deltaBuy = rand.nextInt() % 161 - 80;
            int deltaSell = rand.nextInt() % 161 - 80;
            
            //random update interval (0,1000] milliseconds
            int interval = Math.abs(rand.nextInt() % 1000 + 1); 
            try {
                Thread.sleep((long) interval);
            } catch (InterruptedException e){
                e.printStackTrace();
            }
    
            //create log        
            File log = new File("log"+processID+".txt");
            OutputStream output = new FileOutputStream (log, true);// append to file end

           
            //create a new znode under /root directory, 
            //named by operation num(children num in that directory) 
            List<String> list = zk.getChildren(root, true);
            int update_no = list.size();
            String newZnode = root+"/"+"update"+update_no;
            String update_value = deltaSell+","+deltaBuy;
            if(j==operation_num){//last update is the end message.
                update_value = "updateEnd";
                String log_str = "P"+processID+ " finished.\n";
                output.write(log_str.getBytes());
                System.out.println(log_str);
            }
            zk.create(newZnode, update_value.getBytes(), 
                                    Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            System.out.println("create " + newZnode + " : " + update_value);
            workerClock.increaseStamp();//Lamport requirement

        }
        } catch (KeeperException e){
            e.printStackTrace();
        } catch (InterruptedException e){
            e.printStackTrace();
        } catch (IOException e){
            e.printStackTrace();
        }
    }

}
