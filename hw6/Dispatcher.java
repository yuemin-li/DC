import java.util.List;
import java.lang.InterruptedException;
import java.lang.Integer;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.IOException;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

public class Dispatcher implements Runnable {
    
    //public String address;
    public ZooKeeper zk;
    
    public String processID;
    public Currency currency;
    public Clock dis_clock;
    public String root; 

    private void closeZKInstance() {
    try {
            zk.close();
        } catch (InterruptedException e) {
            System.out.println("Cannot close ZooKeeper Instance");
            System.out.println("Got an exception:" + e.getMessage());
        }
    }
   


    
    public void run(){
        
        try{
        
        //create log
        File update_log = new File("log"+processID+".txt");
        FileOutputStream output = new FileOutputStream(update_log, true);
        
        int end_counter = 0;
        int update_counter = 0;
        while (true){//read update value from /root/updateX znodes 
            if(zk.exists(root+"/"+"update"+update_counter, false)!=null){
                String update_value = new String(zk.getData(root+"/"+"update"+update_counter, false, null));
                update_counter++;
                if(update_value.equals("updateEnd")){
                    end_counter++;
                } else {
                    String[] deltas = update_value.split(",");
                    currency.setSellRate(currency.getSellRate() + Integer.parseInt(deltas[0]));
                    currency.setBuyRate(currency.getBuyRate() + Integer.parseInt(deltas[1]));
                    //log
                    String log_update = TotalOrderZK.realTime() + " [OP" + (update_counter+1)
                                                + ":C" + dis_clock.counter
                                                + "] Currency value is set to ("
                                                + currency.getSellRate() + ", "
                                                + currency.getBuyRate() + ") by ("
                                                + deltas[0] + ", "
                                                + deltas[1] + ").\n";
                    output.write(log_update.getBytes());
                    System.out.println(log_update);
                }
                if(end_counter == 3){//the num of end msg
                    String log_end = "All finished. P" + processID +" is terminating...\n";
                    output.write(log_end.getBytes());
                    System.out.println(log_end);
                    closeZKInstance();
                    System.exit(0);
                }   
            }
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
