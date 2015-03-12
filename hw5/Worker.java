
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Random;
import java.net.Socket;


/* worker thread, sending update and ack msg to processes */
public class Worker implements Runnable{
    
    public ArrayList<Socket> clients;
    public int processID;
    public int operation_num;
    public Clock workerClock;//the local logical clock, not the real clock.    
    
    public void run(){
        
        //every client has its own output stream to store outgoing msg.
        ArrayList<DataOutputStream> out_streams = new ArrayList<DataOutputStream>();
        for(int i=0; i<clients.size(); i++){
            Socket client = clients.get(i);
            DataOutputStream out_stream = new DataOutputStream(client.getOutputStream());
            out_streams.add(out_stream); 
        }
        
        //proceed with operations
        for(int j=0; j<operation_num+1; j++){//last update is a end msg
            //random generate
            Random rand = new Random();
            // rand%161 --> [0, 160], -80 --> [-80, 80]
            int deltaBuy = rand.nextInt() % 161 - 80;
            int deltaSell = rand.nextInt() % 161 - 80;

            int type = 1;//update
            if (j==operation_num){//ending msg
                type = 0;
            }
            String ID = processID + "-" + j;
            workerClock.increaseStamp();//Lamport requirement
        
            Msg msg = new Msg();
            msg.ID = ID;
            msg.type = type;
            msg.processID = processID;
            msg.deltaBuy = deltaBuy;
            msg.deltaSell = deltaSell;
            msg.timestamp = workerClock.counter + 0.1*processID;//vetor clock counter.0, counter.1, conter.2
            //msg.ack_num;
        
            //other thread might also operate on msgQ
            synchronized (Dispatcher.msgQ){
                Dispatcher.msgQ.add(msg);
            }
            
            //write to outputStreams 
            for (int k=0; k<out_streams.size(); k++){
                String msg_str = msg.sendingMsg();
                Socket client = clients.get(k);
                out_streams.get(k).writeUTF(msg_str);//write string instead of write bytes
            }

        }
        
        

    }

}
