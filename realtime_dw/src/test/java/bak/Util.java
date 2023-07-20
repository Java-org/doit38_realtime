package bak;

import java.io.*;

public class Util {

    public static void main(String[] args) throws IOException {

        BufferedReader br = new BufferedReader(new FileReader("d:/x.csv"));

        BufferedWriter bw = new BufferedWriter(new FileWriter("d:/y.txt"));


        String line;
        while(   (line=br.readLine()) != null){
            String[] split = line.split(",");
            String urlRowKey = split[0];
            String pt = split[1];
            String sv = split[2];

            String cmd1 = "put 'dim_page_info','"+urlRowKey+"','f:pt',"+"'"+pt+"'";
            String cmd2 = "put 'dim_page_info','"+urlRowKey+"','f:sv',"+"'"+sv+"'";

            bw.write(cmd1);
            bw.newLine();
            bw.write(cmd2);
            bw.newLine();

        }


        bw.close();
        br.close();



    }

}
