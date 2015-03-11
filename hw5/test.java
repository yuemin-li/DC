import java.io.IOException;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class test{

    public static void main(String args[]) throws IOException{
        File layer1 = new File("info.txt");
        Scanner l1scanner = new Scanner(layer1);
            while (l1scanner.hasNextLine()){//layer1
                String line1 = l1scanner.nextLine();
                //mapping[0] = "domain name", mapping[1] = ip addr.
                String[] mapping1 = line1.split("\\s+");
                //System.out.println(mapping1[2]);
                System.out.println(mapping1[0]);
                System.out.println(mapping1[1]);
                //System.out.println(mapping1);

            }
    }

}

