package udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import sun.font.TrueTypeFont;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;


public class Vmap2Geohashs extends UDF {
    Line2Geohashs l2g = new Line2Geohashs();
    Geohash2CtPoint g2cp = new Geohash2CtPoint();
    Border2Geohashs b2g = new Border2Geohashs();

    public Text evaluate(Text vmapT, Text levelT){
        String geohashs = evaluateS(vmapT,levelT);
        return new Text(geohashs);
    }

    public String evaluateS(Text vmapT, Text levelT){
        if (vmapT == null || levelT == null){
            return "geohash";
        }
        String vmapS = vmapT.toString();
        String[] lnglatA;
        try {
            lnglatA=vmapS.split(";");
        }catch (Exception e){
            return "geohash";
        }
//      计算最大最小经纬度
        double minlng = 180;
        double maxlng = -180;
        double minlat = 90;
        double maxlat = -90;
        for (String lnglatS:lnglatA) {
            try {
                lnglatS = lnglatS.trim();
                String[] lngAndLatA = lnglatS.split(" ");
                String lngS = lngAndLatA[0];
                String latS = lngAndLatA[1];
                double lngD = Double.parseDouble(lngS);
                double latD = Double.parseDouble(latS);
                if (lngD < minlng){
                    minlng = lngD;
                }
                if (lngD >maxlng){
                    maxlng = lngD;
                }
                if (latD < minlat){
                    minlat = latD;
                }
                if (latD > maxlat){
                    maxlat = latD;
                }
            } catch (Exception e){
                return "geohash";
            }
        }
        String minlngS = String.valueOf(minlng);
        String maxlngS = String.valueOf(maxlng);
        String minlatS = String.valueOf(minlat);
        String maxlatS = String.valueOf(maxlat);
        String leftLine = String.format("%s %s;%s %s",minlngS,maxlatS,minlngS,minlatS);
        String rightLine = String.format("%s %s;%s %s",maxlngS,maxlatS,maxlngS,minlatS);

        String leftGeohashs = l2g.evaluateS(new Text(leftLine),levelT);
        String rightGeohashs = l2g.evaluateS(new Text(rightLine),levelT);

        String[] leftGeohashsA = leftGeohashs.split(",");
        String[] rightGeohashsA = rightGeohashs.split(",");

        if(leftGeohashsA.length != rightGeohashsA.length){
            return "geohash";
        }

//        计算边框geohash
        String borderGeohashs = b2g.evaluateS(vmapT,levelT);
        String[] borderGeohashsA = borderGeohashs.split(",");
        List borderGeohashsL = Arrays.asList(borderGeohashsA);
        List<String> resultGeohashsL = new LinkedList<String>();
        for(int i=0;i<leftGeohashsA.length;i++){
            String currentLeftGeohash = leftGeohashsA[i];
            String currentRightGeohash = rightGeohashsA[i];
//            System.out.print(currentLeftGeohash+" "+currentRightGeohash+"\n");
            String currentLeftCtLnglat = g2cp.evaluateS(new Text(currentLeftGeohash));
            String currentRightCtLnglat = g2cp.evaluateS(new Text(currentRightGeohash));
            String currentRowLine = String.format("%s;%s",currentLeftCtLnglat,currentRightCtLnglat);
//            System.out.print(currentRowLine+"\n");
            String currentRowGeohashs = l2g.evaluateS(new Text(currentRowLine),levelT);
//            System.out.print(currentRowGeohashs+"\n");
            String[] currentRowGeohashsA = currentRowGeohashs.split(",");
            int containCount = 0;
            boolean containSwitch = false;
            List<String> borderBorderGeohashsL = new LinkedList<String>();
            List<String> borderMidGeohashsL=new LinkedList<String>();
            List<String> borderMidTempGeohashsL=new LinkedList<String>();
            for (String geohash:currentRowGeohashsA) {
                if (containSwitch){
                    borderMidTempGeohashsL.add(geohash);
                }
                if (borderGeohashsL.contains(geohash)) {
                    containCount++;
                    containSwitch = !containSwitch;
//                    闭口的时候，就不用往borderBorderGeohashsL中加数据了
                    if (!containSwitch){
                        borderMidGeohashsL.addAll(borderMidTempGeohashsL);
                        borderMidTempGeohashsL.clear();
                    }else{
                        borderBorderGeohashsL.add(geohash);
                    }
                }
            }
            resultGeohashsL.addAll(borderBorderGeohashsL);
            resultGeohashsL.addAll(borderMidGeohashsL);
        }
        String resultGeohashsS = String.join(",",resultGeohashsL);
        return resultGeohashsS;
    }

//    public static void main(String []args){
//        Vmap2Geohashs v2g = new Vmap2Geohashs();
//        Text vmap = new Text("114 34;115 35;116 36;114 34");
//        Text level = new Text("6");
//        String resultS = v2g.evaluateS(null,level);
//        System.out.print(resultS);
//    }
}
