package udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.math.BigDecimal;
import java.util.*;

public class Line2Geohashs extends UDF {
    Point2Geohash p2g = new Point2Geohash();
    Geohash2CtPoint g2cp = new Geohash2CtPoint();
    Geohash2AngPoints g2ap = new Geohash2AngPoints();


    public Text evaluate(Text vmapT, Text levelT) {
        String result = evaluateS(vmapT, levelT);
        return new Text(result);
    }

    public String evaluateS(Text vmapT, Text levelT) {
        if (vmapT == null || levelT == null){
            return "geohash";
        }
        String vmapS = vmapT.toString().trim();
        String levelS = levelT.toString().trim();
        int levelI = Integer.parseInt(levelS);
        String lnglat1S = "";
        String lnglat2S = "";
        String lng1S = "";
        String lat1S = "";
        String lng2S = "";
        String lat2S = "";
        double lng1D = 0;
        double lat1D = 0;
        double lng2D = 0;
        double lat2D = 0;
        try {
            String[] lnglatA = vmapS.split(";");
            lnglat1S = lnglatA[0];
            lnglat2S = lnglatA[1];
            lng1S = lnglatA[0].split(" ")[0];
            lat1S = lnglatA[0].split(" ")[1];
            lng2S = lnglatA[1].split(" ")[0];
            lat2S = lnglatA[1].split(" ")[1];
            lng1D = Double.parseDouble(lng1S);
            lat1D = Double.parseDouble(lat1S);
            lng2D = Double.parseDouble(lng2S);
            lat2D = Double.parseDouble(lat2S);
        } catch (Exception e) {
            return "geohash";
        }
        double latSub = lat2D - lat1D;
        double lngSub = lng2D - lng1D;
        double slope;
        String geohash1 = p2g.evaluateS(new Text(lng1S), new Text(lat1S), levelT);
        String geohash2 = p2g.evaluateS(new Text(lng2S), new Text(lat2S), levelT);
//        System.out.print(geohash1+" "+geohash2+"\n");

        String box1 = g2ap.evaluateS(new Text(geohash1));
        String box2 = g2ap.evaluateS(new Text(geohash2));
        String[] box1A = box1.split(";");
        String[] box2A = box2.split(";");
        String box1A_leftDown = box1A[0];
        String box1A_leftTop = box1A[1];
        String box1A_rightTop = box1A[2];
        String box1A_rightDown = box1A[3];
        String box2A_leftDown = box2A[0];
        String box2A_leftTop = box2A[1];
        String box2A_rightTop = box2A[2];
        String box2A_rightDown = box2A[3];
        double boxLngLength = Double.parseDouble(box1A_rightTop.split(" ")[0]) - Double.parseDouble(box1A_leftTop.split(" ")[0]);
        double boxLatLength = Double.parseDouble(box1A_leftTop.split(" ")[1]) - Double.parseDouble(box1A_leftDown.split(" ")[1]);
        double boxSlope = Math.abs(boxLatLength/boxLngLength);
        double boxLngLengthHalf = boxLngLength / 2;
        double boxLatLengthHalf = boxLatLength / 2;
        double bigBoxBeginLng = 0;
        double bigBoxEndLng = 0;
        double bigBoxBeginLat = 0;
        double bigBoxEndLat = 0;
        if ((lng2D - lng1D) < 0) {
            if ((lat2D - lat1D) > 0) {
                bigBoxBeginLng = Double.parseDouble(box1A_rightDown.split(" ")[0]);
                bigBoxBeginLat = Double.parseDouble(box1A_rightDown.split(" ")[1]);
                bigBoxEndLng = Double.parseDouble(box2A_leftTop.split(" ")[0]);
                bigBoxEndLat = Double.parseDouble(box2A_leftTop.split(" ")[1]);

            } else {
                bigBoxBeginLng = Double.parseDouble(box1A_rightTop.split(" ")[0]);
                bigBoxBeginLat = Double.parseDouble(box1A_rightTop.split(" ")[1]);
                bigBoxEndLng = Double.parseDouble(box2A_leftDown.split(" ")[0]);
                bigBoxEndLat = Double.parseDouble(box2A_leftDown.split(" ")[1]);
            }
        } else {
            if ((lat2D - lat1D) > 0) {
                bigBoxBeginLng = Double.parseDouble(box1A_leftDown.split(" ")[0]);
                bigBoxBeginLat = Double.parseDouble(box1A_leftDown.split(" ")[1]);
                bigBoxEndLng = Double.parseDouble(box2A_rightTop.split(" ")[0]);
                bigBoxEndLat = Double.parseDouble(box2A_rightTop.split(" ")[1]);
            } else {
                bigBoxBeginLng = Double.parseDouble(box1A_leftTop.split(" ")[0]);
                bigBoxBeginLat = Double.parseDouble(box1A_leftTop.split(" ")[1]);
                bigBoxEndLng = Double.parseDouble(box2A_rightDown.split(" ")[0]);
                bigBoxEndLat = Double.parseDouble(box2A_rightDown.split(" ")[1]);
            }
        }
        List<String> targetLnglat = new LinkedList();
        int direction;
        if (lngSub != 0) {
            slope = latSub / lngSub;
            double b = lat1D - slope * lng1D;
            if (Math.abs(slope) < boxSlope) {
                if (lngSub > 0) {
                    direction = 1;
                } else {
                    direction = -1;
                }
                double traverseTimeD = Math.abs((bigBoxEndLng - bigBoxBeginLng) / boxLngLength);
                int traverseTimeI = (int) traverseTimeD;
                for (int i = 1; i < traverseTimeI; i++) {
                    double stepLng = bigBoxBeginLng + direction * i * boxLngLength;
                    double stepLat = b + slope * stepLng;
                    double stepLngLeft = stepLng - boxLngLengthHalf;
                    double stepLngRight = stepLng + boxLatLengthHalf;
                    String stepLnglatLeft = String.format("%f %f", stepLngLeft, stepLat);
                    String stepLnglatRight = String.format("%f %f", stepLngRight,stepLat);
                    targetLnglat.add(stepLnglatLeft);
                    targetLnglat.add(stepLnglatRight);
                }
            }else{
                if (latSub > 0) {
                    direction = 1;
                } else {
                    direction = -1;
                }
                double traverseTimeD = Math.abs((bigBoxEndLat - bigBoxBeginLat) / boxLatLength);
                int traverseTimeI = (int) traverseTimeD;
                for (int i = 1; i < traverseTimeI; i++) {
                    double stepLat = bigBoxBeginLat + direction * i * boxLatLength;
                    double stepLng = (stepLat - b)/slope;
                    double stepLatDown = stepLat - boxLatLengthHalf;
                    double stepLatTop = stepLat + boxLatLengthHalf;
                    String stepLnglatDown = String.format("%f %f", stepLng, stepLatDown);
                    String stepLnglatTop = String.format("%f %f", stepLng,stepLatTop);
                    targetLnglat.add(stepLnglatDown);
                    targetLnglat.add(stepLnglatTop);
                }
            }
        }else{
            if (latSub > 0) {
                direction = 1;
            } else {
                direction = -1;
            }
            double traverseTimeD = Math.abs((bigBoxEndLat - bigBoxBeginLat) / boxLatLength);
            int traverseTimeI = (int) traverseTimeD;
            for (int i = 1; i < traverseTimeI; i++) {
                double stepLat = bigBoxBeginLat + direction * i * boxLatLength;
                double stepLng = lng1D;
                double stepLatDown = stepLat - boxLatLengthHalf;
                double stepLatTop = stepLat + boxLatLengthHalf;
                String stepLnglatDown = String.format("%f %f", stepLng, stepLatDown);
                String stepLnglatTop = String.format("%f %f", stepLng,stepLatTop);
                targetLnglat.add(stepLnglatDown);
                targetLnglat.add(stepLnglatTop);
            }
        }

        Set<String> targetGeohash = new LinkedHashSet<String>();
        targetGeohash.add(geohash1);
        for (String currentLnglatS: targetLnglat) {
            String currentLngS = currentLnglatS.split(" ")[0];
            String currentLatS = currentLnglatS.split(" ")[1];
            String currentGeohashS = p2g.evaluateS(new Text(currentLngS),new Text(currentLatS),levelT);
            targetGeohash.add(currentGeohashS);
        }
        targetGeohash.add(geohash2);
        return StringUtils.join(targetGeohash,",");
    }
    /*
     * old function
     * */
//    public String evaluateS(Text vmapT,Text levelT){
//        //        十级栅格参数数组
//        BigDecimal[] lngGridA= {
//                new BigDecimal(45),
//                new BigDecimal(11.25),
//                new BigDecimal(1.40625),
//                new BigDecimal(0.3515625),
//                new BigDecimal(0.0439453125),
//                new BigDecimal(0.010986328125),
//                new BigDecimal(0.001373291015625),
//                new BigDecimal(0.00034332275390625),
//                new BigDecimal("4.2915344238281e-5"),
//                new BigDecimal("1.072883605957e-5"),
//        };
//        BigDecimal[] latGridA={
//                new BigDecimal(45),
//                new BigDecimal(5.625),
//                new BigDecimal(1.40625),
//                new BigDecimal(0.17578125),
//                new BigDecimal(0.0439453125),
//                new BigDecimal(0.0054931640625),
//                new BigDecimal(0.001373291015625),
//                new BigDecimal(0.00017166137695312),
//                new BigDecimal("4.2915344238281e-5"),
//                new BigDecimal("5.3644180297852e-6"),
//        };
//        String vmapS = vmapT.toString().trim();
//        String levelS = levelT.toString().trim();
//        int levelI = Integer.parseInt(levelS);
//        if(vmapS==null||vmapS.length()<=0){
//            return "";
//        }
//
//        String[] svmapS;
//        String slnglat1;
//        String slnglat2;
//        String[] sslnglat1;
//        String[] sslnglat2;
//        String slng1;
//        String slat1;
//        String slng2;
//        String slat2;
//        int x1;
//        int y1;
//        int x2;
//        int y2;
//        int z1;
//        int z2;
//        try{
//
//            svmapS = vmapS.split(";");
//            slnglat1 = svmapS[0];
//            slnglat2 = svmapS[1];
//            sslnglat1 = slnglat1.split(" ");
//            sslnglat2 = slnglat2.split(" ");
//
//            slng1 = sslnglat1[0].trim();
//            slat1 = sslnglat1[1].trim();
//            slng2 = sslnglat2[0].trim();
//            slat2 = sslnglat2[1].trim();
//
//            int geoBinCount = levelI*5;
//            int lngBinCount;
//            int latBinCount;
//            if (levelI%2==0){
//                lngBinCount = geoBinCount/2;
//                latBinCount = geoBinCount/2;
//            }else{
//                lngBinCount = geoBinCount/2+1;
//                latBinCount = geoBinCount/2;
//            }
//            x1 = (int)Math.round(Double.parseDouble(slng1)/lngGridA[levelI-1].doubleValue());
//            y1 = (int)Math.round(Double.parseDouble(slat1)/latGridA[levelI-1].doubleValue());
//            x2 = (int)Math.round(Double.parseDouble(slng2)/lngGridA[levelI-1].doubleValue());
//            y2 = (int)Math.round(Double.parseDouble(slat2)/latGridA[levelI-1].doubleValue());
//            z1 = 0;
//            z2 = 0;
//        }catch (Exception e){
//            return "geohash";
//        }
//
//        StringBuffer resultSB = new StringBuffer();
////        resultSB.append(p2gh.evaluate(new Text(sslnglat1[0]),new Text(sslnglat1[1]),new Text(levelS)).toString()+",");
//
//
//
//        int blocks = 0;
//        boolean cannotUndo = false;
//
//
//        int i, dx, dy, dz, l, m, n, x_inc, y_inc, z_inc, err_1, err_2, dx2, dy2, dz2;
//        int[] pixel = new int[3];
//        pixel[0] = x1;
//        pixel[1] = y1;
//        pixel[2] = z1;
//        dx = x2 - x1;
//        dy = y2 - y1;
//        dz = z2 - z1;
//        x_inc = (dx < 0) ? -1 : 1;
//        l = Math.abs(dx);
//        y_inc = (dy < 0) ? -1 : 1;
//        m = Math.abs(dy);
//        z_inc = (dz < 0) ? -1 : 1;
//        n = Math.abs(dz);
//        dx2 = l << 1;
//        dy2 = m << 1;
//        dz2 = n << 1;
//
//
//        if ((l >= m) && (l >= n)) {
//
//            err_1 = dy2 - l;
//            err_2 = dz2 - l;
//            for (i = 0; i < l; i++) {
//                //DrawOneBlock(player, drawBlock, pixel[0], pixel[1], pixel[2], ref blocks, ref cannotUndo);
////                System.out.println(pixel[0]*4.291534423828125e-05+"-"+pixel[1]*4.291534423828125e-05+"-"+pixel[2]);
////                System.out.println(p2gh.evaluate(new Text(String.valueOf(pixel[0]*4.291534423828125e-05)+" "+String.valueOf(pixel[1]*4.291534423828125e-05))));
//                resultSB.append(p2gh.evaluate(new Text(String.valueOf(pixel[0]*lngGridA[levelI-1].doubleValue())),new Text(String.valueOf(pixel[1]*latGridA[levelI-1].doubleValue())),new Text(levelS)).toString()+",");
//
//                if (err_1 > 0) {
//                    pixel[1] += y_inc;
//                    err_1 -= dx2;
//                }
//                if (err_2 > 0) {
//                    pixel[2] += z_inc;
//                    err_2 -= dx2;
//                }
//                err_1 += dy2;
//                err_2 += dz2;
//                pixel[0] += x_inc;
//            }
//        } else if ((m >= l) && (m >= n)) {
//            err_1 = dx2 - m;
//            err_2 = dz2 - m;
//            for (i = 0; i < m; i++) {
//                //DrawOneBlock(player, drawBlock, pixel[0], pixel[1], pixel[2], ref blocks, ref cannotUndo);
////                System.out.println(pixel[0]*4.291534423828125e-05+"-"+pixel[1]*4.291534423828125e-05+"-"+pixel[2]);
////                System.out.println(p2gh.evaluate(new Text(String.valueOf(pixel[0]*4.291534423828125e-05)+" "+String.valueOf(pixel[1]*4.291534423828125e-05))));
//                resultSB.append(p2gh.evaluate(new Text(String.valueOf(pixel[0]*lngGridA[levelI-1].doubleValue())),new Text(String.valueOf(pixel[1]*latGridA[levelI-1].doubleValue())),new Text(levelS)).toString()+",");
//
//                if (err_1 > 0) {
//                    pixel[0] += x_inc;
//                    err_1 -= dy2;
//                }
//                if (err_2 > 0) {
//                    pixel[2] += z_inc;
//                    err_2 -= dy2;
//                }
//                err_1 += dx2;
//                err_2 += dz2;
//                pixel[1] += y_inc;
//            }
//        } else {
//            err_1 = dy2 - n;
//            err_2 = dx2 - n;
//            for (i = 0; i < n; i++) {
//                //DrawOneBlock(player, drawBlock, pixel[0], pixel[1], pixel[2], ref blocks, ref cannotUndo);
////                System.out.println(pixel[0]*4.291534423828125e-05+"-"+pixel[1]*4.291534423828125e-05+"-"+pixel[2]);
////                System.out.println(p2gh.evaluate(new Text(String.valueOf(pixel[0]*4.291534423828125e-05)+" "+String.valueOf(pixel[1]*4.291534423828125e-05))));
//                resultSB.append(p2gh.evaluate(new Text(String.valueOf(pixel[0]*lngGridA[levelI-1].doubleValue())),new Text(String.valueOf(pixel[1]*latGridA[levelI-1].doubleValue())),new Text(levelS)).toString()+",");
//
//                if (err_1 > 0) {
//                    pixel[1] += y_inc;
//                    err_1 -= dz2;
//                }
//                if (err_2 > 0) {
//                    pixel[0] += x_inc;
//                    err_2 -= dz2;
//                }
//                err_1 += dy2;
//                err_2 += dx2;
//                pixel[2] += z_inc;
//            }
//        }
//        resultSB.append(p2gh.evaluate(new Text(sslnglat2[0]),new Text(sslnglat2[1]),new Text(levelS)).toString());
//        return resultSB.toString();
//    }

//    public static void main(String[] args) {
//        Line2Geohashs tll2geo = new Line2Geohashs();
//        Text lng1 = new Text("113 35;115 36");
//        System.out.println(tll2geo.evaluate(lng1, new Text("9")).toString());
//    }
}
