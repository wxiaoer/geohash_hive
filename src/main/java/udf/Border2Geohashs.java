package udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class Border2Geohashs extends UDF {
    Line2Geohashs l2ghs = new Line2Geohashs();

    public Text evaluate(Text vmap,Text levelT){
        String geohashs = evaluateS(vmap,levelT);
        return new Text(geohashs);
    }

    public String evaluateS(Text vmap, Text levelT){
        if (vmap == null || levelT == null){
            return "geohash";
        }
        String svmap = vmap.toString();
        String[] sslnglat;
        try {
            sslnglat=svmap.split(";");
        }catch (Exception e){
            return "geohash";
        }
        StringBuffer geohashsSB = new StringBuffer();
        for (int i = 0; i<sslnglat.length-1; i++){
            int nextIdx;
            nextIdx = i+1;

            String twolnglat = sslnglat[i]+";"+sslnglat[nextIdx];
            String geohashs = l2ghs.evaluate(new Text(twolnglat),levelT).toString();
            if (i != 0){
                geohashsSB.append(",");
                geohashs=geohashs.substring(geohashs.indexOf(",")+1,geohashs.length());
            }
            geohashsSB.append(geohashs);
//            System.out.println(geohashsSB.length());
        }
        return geohashsSB.toString();
    }

//    public static void main(String[] args){
//        Border2Geohashs border2ghs = new Border2Geohashs();
//        Text test = new Text("114 34;115 35;116 36");
//        System.out.println(border2ghs.evaluateS(test,new Text("4")));
//        System.out.println(border2ghs.evaluate(test,new Text("4")).toString());
//    }
}
