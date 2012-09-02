311 Dashboard
=============

The 311 Dashboard helps you visualize and analyze 311 service request data. It is powered
by the Open311 API.

Let's create a PostGIS-enabled database and import San Francisco's neighborhood data.

The following instructions are known to work on a machine running Ubuntu 11.10 (Oneiric).

Adjust access permissions
-------------------------
1. `sudo vim /etc/postgresql/9.1/main/pg_hba.conf`
2. Change lines 85, 90, 92, 94: first two methods were "peer", last two were "md5", changes both to "trust"
3. `sudo /etc/init.d/postgresql restart`

Create postgres user
--------------------
1. `sudo -u postgres createuser sf_311`
2. In the postgres database: `ALTER user sf_311 WITH password 'password';`

Create a 311 database with the correct user
-------------------------------------------
1. `sudo -u postgres createdb -O sf_311 -E utf8 sf_311`
(Create a database called sf_311 with owner sf_311)

Set up the database with a PostGIS template
-------------------------------------------
1. `psql -U sf_311 -d sf_311 -f /usr/share/postgresql/9.1/contrib/postgis-2.1/postgis.sql`
2.  `psql -U sf_311 -d sf_311 -f /usr/share/postgresql/9.1/contrib/postgis-2.1/spatial_ref_sys.sql`

Get the neighborhood data
-------------------------
1. Download the neighborhood shapes from San Francisco here:

    http://apps.sfgov.org/datafiles/view.php?file=sfgis/planning_neighborhoods.zip
2. Unzip planning_neighborhoods.zip

Convert the shapefile to web mercator
-------------------------------------
0. `ogrinfo -so planning_neighborhoods.shp`
1. `ogr2ogr -t_srs EPSG:900913 planning_neighborhoods_900913.shp planning_neighborhoods.shp`
2. `ogr2ogr -f 'ESRI Shapefile' -t_srs EPSG:4326 planning_neighborhoods_4326.shp planning_neighborhoods.shp`

Import the Shapefile into a PostGIS-enabled database
----------------------------------------------------
1. `shp2pgsql -dID -s 900913 -W latin1 planning_neighborhoods_900913.shp pn_geoms | psql -U sf_311`