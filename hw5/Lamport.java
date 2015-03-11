
import java.io.IOException;
import java.io.File;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Scanner;
import java.net.ServerSocket;
import java.net.Socket;


/* entry of the program */
public class Lamport {
    public static void main(String args[]) throws IOException {
        //parsing args
        int processID = Integer.parseInt(args[0]);
        int processNum = Integer.parseInt(args[1]);
        int clockRate = Integer.parseInt(args[2]);
        
        //init currency, initial value is (100,100)
        Currency currency = new currency();
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
        
        //start local clock
        LocalClock lc = new Localclock();
        lc.localClock = clock;
        Thread lcthread = new Thread(lc);
        lcthread.start();
        
        //parsing own addr
        String myaddr = addrs.get(processID);
        String[] ipnport = myaddr.split("\\s+");
        String my_ip = ipnport[0];
        String my_port = ipnport[1]; 
  
        //set up serversocket to listen for msg
        ServerSocket socket_s = new ServerSocket(my_port);
         




 
    }
}
