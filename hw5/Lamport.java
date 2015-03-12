
import java.io.IOException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.Calendar;
import java.net.ServerSocket;
import java.net.Socket;


/* entry of the program */
public class Lamport {
    public static void main(String args[]) throws IOException {
        //parsing args
        int processID = Integer.parseInt(args[0]);
        int operation_num = Integer.parseInt(args[1]);
        int clockRate = Integer.parseInt(args[2]);
        
        //init currency, initial value is (100,100)
        Currency currency = new Currency();
        currency.setBuyRate(100);
        currency.setSellRate(100);

        //init clock
        Clock clock = new Clock();
        clock.setClockRate(clockRate);

        //read info.txt file
        File info = new File("info.txt");
        ArrayList<String> addrs = new ArrayList<String>();
        try{
            Scanner scanner_info = new Scanner(info);
            String addr = "";
            while (scanner_info.hasNextLine()){
                addr = scanner_info.nextLine();
                addrs.add(addr);
            }
            scanner_info.close();
        } catch (FileNotFoundException e){
            e.printStackTrace();
        }

        //ceate log file
        File log = new File("log"+processID+".txt");
        OutputStream output = new FileOutputStream (log, true);// append to file end
        
        //write info to log
        for(int i=0; i<addrs.size(); i++){
            String oneaddr = addrs.get(i);
            String[] ip_port_pair = oneaddr.split("\\s+");
            String log_info = "[P"+i+"]"+ip_port_pair[0]+":"+ip_port_pair[1]+"\n";
            output.write(log_info.getBytes());
            System.out.println(log_info); 
        }        

        //start local clock
        LocalClock lc = new LocalClock();
        lc.localClock = clock;
        Thread lcthread = new Thread(lc);
        lcthread.start();
        
        //parsing own addr
        String myaddr = addrs.get(processID);
        String[] ipnport = myaddr.split("\\s+");
        String my_ip = ipnport[0];
        int my_port = Integer.parseInt(ipnport[1]); 
  
        //set up serversocket to listen for msg, and write listening to log
        ServerSocket socket_s = new ServerSocket(my_port);
        String log_listen = "P"+processID+"("+my_ip+") is listening on port "+my_port+" ...\n";
        output.write(log_listen.getBytes());
        System.out.println(log_listen);
       
        //set up client sockets
        ArrayList<Socket> socket_c = new ArrayList<Socket>(); 
        int accept_c_num = 2 - processID;// the num of accept client sockets for server
        int remote_c_num = processID;// the num of remote client socket to open
        String allconnected = "All connected.\n";
        String waiting = "Waiting for all to be connected...\n";
        
        //open up accept client sockets
        while (accept_c_num > 0){
            Socket accept_c = socket_s.accept();
            accept_c.setTcpNoDelay(true);//small packet, no need to wait for full buffer
            socket_c.add(accept_c);
            String connectedfrom = realTime()+"P"+processID+" is connected from "+"P"+(accept_c_num)
                                    +"("+accept_c.getInetAddress().toString() +")\n";
            output.write(connectedfrom.getBytes());
            System.out.println(connectedfrom);
            accept_c_num--;
            
            //check connection status
            if(accept_c_num==0 && remote_c_num==0){//no more connection work to to
                output.write(allconnected.getBytes());
                System.out.println(allconnected); 
            } else {
                output.write(waiting.getBytes());
                System.out.println(waiting);
            }
            
        } 

        //open up remote client sockets
        while(remote_c_num > 0){
            String remote_addr = addrs.get(remote_c_num-1);
            String[] remote_ipnport = remote_addr.split("\\s+");
            String remote_ip = remote_ipnport[0];
            int remote_port = Integer.parseInt(remote_ipnport[1]);
            Socket remote_c = new Socket(remote_ip, remote_port);
            socket_c.add(remote_c);
            String connectto = realTime()+"P"+processID+" is connected to "+"P"+(remote_c_num-1)
                                    +"("+remote_c.getInetAddress().toString() +")\n";
            output.write(connectto.getBytes());
            System.out.println(connectto);
            remote_c_num--;
            
            //check connection status
            if(accept_c_num==0 && remote_c_num==0){//no more connection work to to
                output.write(allconnected.getBytes());
                System.out.println(allconnected);
            } else {
                output.write(waiting.getBytes());
                System.out.println(waiting);
            }
 
        }
                



 
    }

    private static String realTime(){
        Calendar c = Calendar.getInstance();
        //[month/date hour:minute:second]
        String realTime = "["+c.get(Calendar.MONTH)+"/"+c.get(Calendar.DATE)+" "
                            +c.get(Calendar.HOUR_OF_DAY)+":"+c.get(Calendar.MINUTE)+":"+c.get(Calendar.SECOND)+"]";
        return realTime;
    }
}
