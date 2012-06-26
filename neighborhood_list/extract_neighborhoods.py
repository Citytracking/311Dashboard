import shapefile # Available from http://code.google.com/p/pyshp/

# Shapefile for planning neighborhoods is available from datasf.org
# The shapefile was reprojected using ogr2ogr: 
# ogr2ogr -f 'ESRI Shapefile' -t_srs EPSG:4326 planning_neighborhoods_4326.shp planning_neighborhoods.shp

shp_path = 'shapes/planning_neighborhoods/planning_neighborhoods_4326.shp'

sf_shapefile = shapefile.Reader(shp_path)

sf_records = sf_shapefile.shapeRecords()

neighborhoods = [sf_record.record[0] for sf_record in sf_records]

with open('neighborhoods.txt', 'wt') as f:
  f.write(str(neighborhoods))


