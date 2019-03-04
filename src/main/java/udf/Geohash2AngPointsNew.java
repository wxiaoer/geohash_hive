package udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.text.DecimalFormat;

public class Geohash2AngPointsNew extends UDF {
    private static final String charMap = "0123456789bcdefghjkmnpqrstuvwxyz";
    public Text evaluate(Text geohash){
        String result = evaluateS(geohash);
        return new Text(result);
    }

    public String evaluateS(Text geohash){
        if (geohash == null){
            return "geohash";
        }
        String strGeohash = geohash.toString();
        boolean isLat = false;
        double latMax = 90;
        double latMin = -90;
        double lonMax = 180;
        double lonMin = -180;
        for(int i = 0; i < strGeohash.length(); i++){
            Integer currentInt = new Integer(charMap.indexOf(strGeohash.charAt(i)));
            String s = Integer.toBinaryString(currentInt);
            String ss = "00000"+s;
            ss = ss.substring(s.length());
            for(int j = 0; j<ss.length(); j++){
                char currentChar = ss.charAt(j);
                if(isLat){
                    if(currentChar=='1'){
                        latMin = (latMax+latMin)/2;
                    }else{
                        latMax = (latMax+latMin)/2;
                    }
                }else{
                    if(currentChar=='1'){
                        lonMin = (lonMax+lonMin)/2;
                    }else{
                        lonMax = (lonMax+lonMin)/2;
                    }
                }
                isLat = !isLat;
            }
        }
        DecimalFormat df = new DecimalFormat("#.00000000");
        String minLng = df.format(lonMin);
        String maxLng = df.format(lonMax);
        String minLat = df.format(latMin);
        String maxLat = df.format(latMax);
        String result = String.format("%s#%s|%s#%s|%s#%s|%s#%s|%s#%s", minLng,minLat,minLng,maxLat,maxLng,maxLat,maxLng,minLat,minLng,minLat);
        return result;
    }

//    public static void main(String[] args){
//        Geohash2AngPoints gh2ll = new Geohash2AngPoints();
//        Text geohash = new Text("ww1seey1e");
//        System.out.println(gh2ll.evaluate(geohash).toString());
//    }
}
