package udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class Geohash2Surrend extends UDF{
    Geohash2AngPoints g2aps = new Geohash2AngPoints();
    Point2Geohash p2g = new Point2Geohash();

    /*
    * @param 要解析geohash，经度上扩展的列数，维度上扩展的行数
    * */
    public Text evaluate(Text geohash, Text lngOutAmount, Text latOutAmount) {
        String geohashArrStr = evaluateS(geohash,lngOutAmount,latOutAmount);
        return new Text(geohashArrStr);
    }

    public String evaluateS(Text geohash, Text lngOutAmount, Text latOutAmount) {
        if (geohash == null || lngOutAmount == null || latOutAmount == null){
            return "geohash";
        }
        int geohashLevelInt = geohash.toString().length();
        Text geohashLevel = new Text(String.valueOf(geohashLevelInt));

        int lngOutAmountInt = Integer.parseInt(lngOutAmount.toString());
        int latOutAmountInt = Integer.parseInt(latOutAmount.toString());

        Text angPoints = g2aps.evaluate(geohash);
        String angPointsStr = angPoints.toString();
        String[] angPointsArr = angPointsStr.split(";");
        String minLngMinLat = angPointsArr[0];
        String maxLngMaxLat = angPointsArr[2];
        Double minLng = new Double(minLngMinLat.split(" ")[0]);
        Double minLat = new Double(minLngMinLat.split(" ")[1]);
        Double maxLng = new Double(maxLngMaxLat.split(" ")[0]);
        Double maxLat = new Double(maxLngMaxLat.split(" ")[1]);
        Double centLng = (minLng+maxLng)/2;
        Double centLat = (minLat+maxLat)/2;
        Double geoLngLength = maxLng - minLng;
        Double geoLatLength = maxLat - minLat;

        int rowAmount = lngOutAmountInt*2+1;
        int colAmount = latOutAmountInt*2+1;
        String[] geohashArr = new String[rowAmount*colAmount];
        int arrayIndex = 0;
        for(int i=0;i<rowAmount;i++){
            for(int j=0;j<colAmount;j++){
                Double currentLng = centLng + (lngOutAmountInt-i)*geoLngLength;
                Double currentLat = centLat + (latOutAmountInt-j)*geoLatLength;
                String currentLngStr = currentLng.toString();
                String currentLatStr = currentLat.toString();
                String currentGeohash = p2g.evaluate(new Text(currentLngStr),new Text(currentLatStr),geohashLevel).toString();
                geohashArr[arrayIndex]=currentGeohash;
                arrayIndex++;
            }
        }
        String geohashArrStr = StringUtils.join(geohashArr,",");
        return geohashArrStr;
    }

//    public static void main(String[] args){
//        Geohash2Surrend g2s9 = new Geohash2Surrend();
//        Text geohash = new Text("ww1seey1e");
//        Text lngOutAmount = new Text("1");
//        Text latOutAmount = new Text("1");
//        System.out.println(g2s9.evaluate(geohash,lngOutAmount,latOutAmount).toString());
//    }
}
