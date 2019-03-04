# geohash_hive
point,line,vmap convert to geohashs
# usage
jar package in folder artifacts can be use immadiate in hive
# simple
geohash2angpoints([geohash]) //geohash转栅格的四个角加中心点经纬度

geohash2ctpoint([geohash]) //geohash转栅格中心经纬度

point2geohash([longitude,latitude,level]) //点经纬度转geohash，level：geohash等级

line2geohashs([longitude latitude;longitude latitude,level]) //线段转经纬度

vmap2geohashs([longitude latitude;longitude latitude;...,level]) //vmap转经纬度

