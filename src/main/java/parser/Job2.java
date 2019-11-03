package parser;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Map.Entry.comparingByKey;
import static java.util.stream.Collectors.toMap;


//Utility created to create histogram for job 2

public class Job2 {

    public static void main(String[] args) throws IOException {

         String trialReq =" curl -X POST -H 'Content-Type: application/json' -d '{\"chart\": {\"type\": \"bar\", \"data\": {\"labels\": [";

        final String dir = "/home/aditya/Downloads/Graphs"; //specify directory where all the output part files are located

        try (Stream<Path> walk = Files.walk(Paths.get(dir))) {
            List<String> outputFiles = walk.filter(Files::isRegularFile).map(x -> x.toString()).collect(Collectors.toList());
            outputFiles.forEach(System.out::println);

            Map<Integer,Integer> entries = new HashMap<>();

            for(String f: outputFiles){ //get the values from each file and store it in a Map
                File file = new File(f);
                BufferedReader br = new BufferedReader(new FileReader(file));

                String s;
                String x[];

                while(( s = br.readLine())!=null){
                    x= s.split(",");
                    if (x.length == 2 && !x[0].equals("")){
                        if(x[0].toString().length()==4){
                            entries.put(Integer.parseInt(x[0].trim()),Integer.parseInt(x[1].trim()));
                        }
                    }
                }
            }
            //sort the results from Map
            Map<Integer,Integer> sorted = entries.entrySet().stream().sorted(comparingByKey())
                    .collect(toMap(e -> e.getKey(), e -> e.getValue(), (e1, e2) -> e2, LinkedHashMap::new));

            String labels= new String();
            String data = new String();

            //generate curl command
            for(Map.Entry<Integer,Integer> entry : sorted.entrySet()){
                labels = labels + "\""+ entry.getKey().toString() +"\",";
                data = data + entry.getValue().toString() +",";
            }
            labels = labels.substring(0,labels.length()-1);
            data = data.substring(0,data.length()-1);
            trialReq = trialReq + labels + "], \"datasets\": [{\"label\": \"Number of publications\", \"data\": ["+ data +"]}]}}}\'  https://quickchart.io/chart/create";

            System.out.println("\n\n\n"+trialReq+"\n");

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
