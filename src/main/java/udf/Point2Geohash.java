package udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class Point2Geohash extends UDF {
    public Text evaluate(Text lngT, Text latT, Text levelT){
        String geohash = evaluateS(lngT,latT,levelT);
        return new Text(geohash);
    }

    public String evaluateS(Text lngT, Text latT, Text levelT){
        if (lngT == null || lngT == null || levelT == null){
            return "geohash";
        }
        String lngS = lngT.toString().trim();
        String latS = latT.toString().trim();
        String levelS = levelT.toString().trim();

        if(lngS==null||lngS.length()<=0||latS==null||latS.length()<=0||levelS==null||levelS.length()<=0){
            return "";
        }
        Double lngD;
        Double latD;
        int levelI;
        try {
            lngD = Double.parseDouble(lngS);
            latD = Double.parseDouble(latS);
            levelI = Integer.parseInt(levelS);
        }catch (Exception e){
            return "geohash";
        }

        String charMap = "0123456789bcdefghjkmnpqrstuvwxyz";
        double latMax = 90;
        double latMin = -90;
        double lonMax = 180;
        double lonMin = -180;
        int[] lngBin;
        int[] latBin;
        int[] geoBin;
        int geoBinCount = levelI*5;
        int lngBinCount;
        int latBinCount;
        if (levelI%2==0){
            lngBinCount = geoBinCount/2;
            latBinCount = geoBinCount/2;
            lngBin = new int[lngBinCount];
            latBin = new int[latBinCount];
            geoBin = new int[geoBinCount];
        }else{
            lngBinCount = geoBinCount/2+1;
            latBinCount = geoBinCount/2;
            lngBin = new int[lngBinCount];
            latBin = new int[latBinCount];
            geoBin = new int[geoBinCount];
        }
        for(int i=0;i<lngBinCount;i++){
            double lonMid = (lonMax+lonMin)/2;
            if (lngD>lonMid){
                lngBin[i]=1;
                lonMin=lonMid;
            }else{
                lngBin[i]=0;
                lonMax=lonMid;
            }
            geoBin[i*2]=lngBin[i];
        }
        for(int i=0;i<latBinCount;i++){
            double latMid = (latMax+latMin)/2;
            if (latD>latMid){
                latBin[i]=1;
                latMin=latMid;
            }else{
                latBin[i]=0;
                latMax=latMid;
            }
            geoBin[i*2+1]=latBin[i];
        }
        StringBuffer geohashSB = new StringBuffer();
        int count5=0;
        StringBuffer countS=new StringBuffer();
        for (int i=0; i<geoBin.length; i++){
            count5+=1;
            countS= countS.append(geoBin[i]);
            if (count5==5){
                count5=0;
                int idx = Integer.valueOf(countS.toString(),2);
                String geoCode = charMap.substring(idx,idx+1);
                geohashSB.append(geoCode);
                countS.setLength(0);
            }
        }
        return geohashSB.toString();
    }

//    public static void main(String[] args) {
//        Point2Geohash pt2gh = new Point2Geohash();
//        Text lngTT = new Text("115");
//        Text latTT = new Text("35");
//        Text levelTT = new Text("6");
//        System.out.println(pt2gh.evaluate(lngTT,latTT,levelTT).toString());
//    }
}
