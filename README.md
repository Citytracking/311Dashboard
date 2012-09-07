311 Dashboard
=============

The 311 Dashboard helps you visualize and analyze 311 service request data. It is powered
by the Open311 API.

The following instructions are known to work on a machine running Ubuntu 11.10 (Oneiric).

Let's create a PostGIS-enabled database and import San Francisco's neighborhood data.

Install Dependencies
--------------------
1. `sudo apt-get update`
2. `sudo apt-get install binutils gdal-bin libproj-dev postgresql-9.1-postgis postgresql-server-dev-9.1 python-psycopg2`
3. `sudo apt-get install gdal-bin`
4. You will also need pip if you don't have it:
    - `sudo apt-get install python-pip`
    - `sudo pip install --upgrade pip`
5. Install the python dependencies:
    - `sudo pip install Flask`
    - `sudo pip install psycopg2`

Adjust access permissions
-------------------------
1. `sudo vim /etc/postgresql/9.1/main/pg_hba.conf`
2. Change lines 85, 90, 92, 94: first two methods were "peer", last two were "md5", changes both to "trust"
3. `sudo /etc/init.d/postgresql restart`

Create postgres user
--------------------
1. `sudo -u postgres createuser sf_311`
2. In the postgres database: `ALTER user sf_311 WITH password 'CHOOSE_PASSWORD';`

Create a 311 database with the correct user
-------------------------------------------
1. `sudo -u postgres createdb -O sf_311 -E utf8 sf_311`
(Create a database called sf_311 with owner sf_311)

Set up the database with a PostGIS template
-------------------------------------------
1. `psql -U sf_311 -d sf_311 -f /usr/share/postgresql/9.1/contrib/postgis-2.1/postgis.sql`
2. `psql -U sf_311 -d sf_311 -f /usr/share/postgresql/9.1/contrib/postgis-2.1/spatial_ref_sys.sql`

Get the neighborhood data
-------------------------
1. Download the neighborhood shapes from San Francisco here:

    http://apps.sfgov.org/datafiles/view.php?file=sfgis/planning_neighborhoods.zip
    
2. Unzip planning_neighborhoods.zip

Convert the shapefile to web mercator
-------------------------------------
0. `ogrinfo -so planning_neighborhoods.shp`
1. `ogr2ogr -f 'ESRI Shapefile' -t_srs EPSG:900913 planning_neighborhoods_900913.shp planning_neighborhoods.shp`

Import the Shapefile into a PostGIS-enabled database
----------------------------------------------------
1. `shp2pgsql -dID -s 900913 -W latin1 planning_neighborhoods_900913.shp pn_geoms | psql -U sf_311`

Let's create a table for all of our 311 request data and populate the table.

Create requests table
---------------------
1. Run the db_setup shell script: `sh db_setup.sh`

Populate requests table
-----------------------
1. Edit the db_config_sample.json to reflect the host, user, password, and database name 
of your setup
2. Initially, try to get the last two week's worth of data with the update_postgres_sf.py script.
If today's date is August 1st, 2013, run `python update_postgres_sf.py -e 2013-08-01 -n 14`
3. Remember that you can use this same script to update the data in the future.

Run the application
-------------------
1. You can find the core of the application, dashboard.py, in the flask directory.
Once you're in the flask directory, simply type `sudo python dashboard.py --port 80`

    If you want to run the application on port 5000, you can type `python dashboard.py --port 5000`