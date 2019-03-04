package udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.text.DecimalFormat;

public class Geohash2CtPoint extends UDF {

    //    定义字符对应MAP
    private static final String CHAR_MAP = "0123456789bcdefghjkmnpqrstuvwxyz";

    public Text evaluate(Text geohash) {
        String result = evaluateS(geohash);
        return new Text(result);
    }

    public String evaluateS(Text geohash) {
        if (geohash == null){
            return "geohash";
        }
        String strGeohash = geohash.toString();
        boolean isLat = false;
        double latMax = 90;
        double latMin = -90;
        double lonMax = 180;
        double lonMin = -180;
        for (int i = 0; i < strGeohash.length(); i++) {
            Integer currentInt = new Integer(CHAR_MAP.indexOf(strGeohash.charAt(i)));
            String s = Integer.toBinaryString(currentInt);
//           用0填充5位二进制数字空白
            String ss = "00000" + s;
            ss = ss.substring(s.length());
            for (int j = 0; j < ss.length(); j++) {
                char currentChar = ss.charAt(j);
                if (isLat) {
                    if (currentChar == '1') {
                        latMin = (latMax + latMin) / 2;
                    } else {
                        latMax = (latMax + latMin) / 2;
                    }
                } else {
                    if (currentChar == '1') {
                        lonMin = (lonMax + lonMin) / 2;
                    } else {
                        lonMax = (lonMax + lonMin) / 2;
                    }
                }
                isLat = !isLat;
            }
        }
//        保留8位小数
        DecimalFormat df = new DecimalFormat("#.00000000");
        String centLng = df.format((lonMax + lonMin) / 2);
        String centLat = df.format((latMax + latMin) / 2);
        String minLng = df.format(lonMin);
        String maxLng = df.format(lonMax);
        String minLat = df.format(latMin);
        String maxLat = df.format(latMax);
        String result = String.format("%s %s", centLng, centLat);
        return result;
    }

//    public static void main(String[] args) {
//        Geohash2CtPoint gh2cp = new Geohash2CtPoint();
//        Text geohash = new Text("ww6kz9");
//        System.out.println(gh2cp.evaluate(geohash).toString());
//    }
}
