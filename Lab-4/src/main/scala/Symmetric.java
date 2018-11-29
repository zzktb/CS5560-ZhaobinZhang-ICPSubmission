import java.io.*;
import java.util.List;
import java.util.*;

/**
 * Created by DJ Yuhn on 11/15/2018
 */
public class Symmetric {
    public static void main(String[] args) throws IOException {
        List<String> tripletsArrayList = new ArrayList<>();
        try {
            File file=new File("data/triplets/sample.txt");
            BufferedReader bufferedReader=new BufferedReader(new FileReader(file));
            String triplet;

            while ((triplet=bufferedReader.readLine()) != null) {
                tripletsArrayList.add(triplet);
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        ArrayList<String> symmetricArray = new ArrayList<>();
        // Symmetric
        for(int i = 0; i < tripletsArrayList.size(); i++) {
            String[] splitTriplet = tripletsArrayList.get(i).split("\t");
            System.out.println(splitTriplet[0].toString());
            for (int j = 1; j < tripletsArrayList.size(); j++) {
                String[] checkSplitTriplet = tripletsArrayList.get(j).split("\t");
                if(splitTriplet.length == 3 && checkSplitTriplet.length == 3) {
                    if (splitTriplet[2].contains(checkSplitTriplet[0]) && splitTriplet[1].contains(checkSplitTriplet[1])
                            && splitTriplet[0].contains(checkSplitTriplet[2])) {
                        symmetricArray.add(tripletsArrayList.get(i) + "\n" + tripletsArrayList.get(j));
                    }
                }
            }
        }

        BufferedWriter symmetricWriter = new BufferedWriter(new FileWriter("data/symmetric.txt"));
        for(String symmetric: symmetricArray) {
            symmetricWriter.append(symmetric);
        }
        symmetricWriter.close();

    }
}