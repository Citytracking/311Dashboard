from flask import Flask, g
from flask import render_template, Response
from flask import request, session, redirect, url_for, abort, flash, json, jsonify, make_response

import psycopg2
import psycopg2.extras

from datetime import date, datetime, timedelta

ONE_DAY = timedelta(days=1)

app = Flask(__name__)
app.config.from_object(__name__)

# Set up Memcache
USE_MEMCACHE = True

if USE_MEMCACHE:
    from werkzeug.contrib.cache import MemcachedCache
    cache = MemcachedCache(['127.0.0.1:11211'])
    
    CACHE_TIMEOUT = 300

def connect_db():
    return psycopg2.connect("host=localhost password=77 dbname=sf_311 user=sf_311")

@app.before_request
def before_request():
    g.db = connect_db()

@app.teardown_request
def teardown_request(exception):
    if hasattr(g, 'db'):
        g.db.close()
###
# Utility functions
###
def query_db(query, args=()):
    cur = g.db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    try:
        #a = cur.mogrify(query,args)
        #print a
        cur.execute(query, args)
    except psycopg2.DataError,err:
        print 'Error', err
    
    try:
        res = cur.fetchall()
    except psycopg2.ProgrammingError, err:
        res = None
            
        print 'Error', err

    cur.close()

    return res

def create_json(attrs, res):
    requests = []

    for row in res:
        requests.append(dict(zip(attrs,row)))

    return json.dumps(requests)

def create_jsonp_response_from_dbresult(res='None', callback='data'):
    if res:
        requests_json = json.dumps(res)

        return create_jsonp_response(requests_json, 'data')
    else:
        return create_null_response()

def create_jsonp_response(requests_json, callback='data'):

    response_jsonp = callback + '(' + requests_json + ');'

    response = make_response(response_jsonp)

    response.headers['Content-Type'] = 'application/javascript; charset=utf-8'

    return response

def create_null_response(callback='data'):
    null_data_response = json.dumps({'response': 'fail', 'message': 'No data found.'})

    return create_jsonp_response(null_data_response, callback)

def parse_date(date_str, date_format='%Y-%m-%d'):
    try:
        return datetime.strptime(date_str, date_format)
    except Exception, e:
        return None

def get_formatted_date(start_date, end_date, fmt='%Y-%m-%d'):
    start_day = start_date.strftime(fmt);
    end_day = end_date.strftime(fmt)
    
    return (start_day, end_day)

def get_max_date():
    res = query_db("""
        SELECT MAX(requested_datetime) AS max_date
        FROM sf_requests
    """)
    
    return res[0]['max_date']

def combine_open_closed_counts(open_count_res, closed_count_res, start_date, end_date):
    """
        Combine counts of open requests and closed counts into one structure.
        You will end up with something that looks like this:
            
        [{"date": "2012-06-30", "Open": 27, "Closed": 133}, 
         {"date": "2012-07-01", "Open": 31, "Closed": 44},...]
    """
    
    daily_count_res = open_count_res + closed_count_res
    
    sorted_daily_count = sorted(daily_count_res, key=lambda k: k['date'])
        
    input_exhausted = False
    
    date = start_date
    
    count = 0
    
    input = sorted_daily_count[count]
    
    next_date = start_date
    
    combined_result = []
    
    while date <= end_date:
        info = {}
        
        info['date'] = datetime.strftime(date, '%Y-%m-%d')
        info['Open'] = 0
        info['Closed'] = 0
        
        while not input_exhausted and info['date'] == input['date']:
            if input['status'] == 'Closed':
                info['Closed'] += input['count']
            elif input['status'] == 'Open':
                info['Open'] += input['count']
                
            count = count + 1
            
            if count >= len(sorted_daily_count):
                input_exhausted = True
            else:
                input = sorted_daily_count[count]
        
        date = date + timedelta(1)
        
        combined_result.append(info)
    
    return combined_result

def convert_neighborhood_slug(slug):
    neighborhoods = {'bayview':'Bayview',
                 'bernal':'Bernal Heights', 
                 'castro':'Castro/Upper Market',
                 'chinatown':'Chinatown',
                 'crocker_amazon':'Crocker Amazon', 
                 'diamond_heights':'Diamond Heights', 
                 'downtown':'Downtown/Civic Center', 
                 'excelsior':'Excelsior', 
                 'financial_district':'Financial District', 
                 'glen_park':'Glen Park', 
                 'gg_park':'Golden Gate Park', 
                 'haight_ashbury':'Haight Ashbury',
                 'inner_richmond': 'Inner Richmond', 
                 'inner_sunset':'Inner Sunset', 
                 'lakeshore':'Lakeshore', 
                 'marina':'Marina', 
                 'mission':'Mission',
                 'nob_hill':'Nob Hill', 
                 'noe_valley':'Noe Valley', 
                 'north_beach':'North Beach', 
                 'ocean_view':'Ocean View',
                 'outer_mission':'Outer Mission', 
                 'outer_richmond':'Outer Richmond', 
                 'outer_sunset':'Outer Sunset', 
                 'pacific_heights':'Pacific Heights',
                 'parkside':'Parkside', 
                 'potrero_hill':'Potrero Hill', 
                 'presidio':'Presidio', 
                 'presidio_heights':'Presidio Heights',
                 'russian_hill':'Russian Hill', 
                 'seacliff':'Seacliff', 
                 'soma':'South of Market', 
                 'ti':'Treasure Island/YBI', 
                 'twin_peaks':'Twin Peaks',
                 'visitacion':'Visitacion Valley', 
                 'west_twin_peaks':'West of Twin Peaks', 
                 'western_addition':'Western Addition'}
                 
    return neighborhoods[slug]

###
# Render each page
###
@app.route("/")
def dashboard():
    """
        Render the main dashboard view.
    """
    return render_template('dashboard.html')

@app.route("/neighborhood/<neighborhood>")
def neighborhood_dashboard(neighborhood=None):
    """
        Render each neighborhood view.
    """
    
    return render_template('neighborhood_dashboard.html', neighborhood=convert_neighborhood_slug(neighborhood))

@app.route("/neighborhoods/")
def neighborhoods_list():
    """
        Display an ordered list of neighborhoods.
    """
    results = query_db("""
        SELECT 
            service_name, service_code, COUNT(*) as count 
        FROM sf_requests 
        WHERE requested_datetime between NOW() - INTERVAL '3 MONTH' AND NOW() 
        GROUP BY service_name, service_code 
        ORDER BY count DESC 
        Limit 15
    """)

    top_neighborhoods = query_db("""
        SELECT 
            COUNT(r.*) as count, p.neighborho 
        FROM sf_requests as r 
        JOIN pn_geoms as p ON ST_INTERSECTS(geom, ST_MakePoint(r.lon,r.lat))
        WHERE r.requested_datetime between now() - INTERVAL '30 DAY' and now() 
        GROUP BY p.neighborho order by count DESC
    """)

    if results:
        requests_json = json.dumps(results)

    res = json.dumps(results)

    return render_template('neighborhoods.html', results=res, top_neighborhoods=top_neighborhoods)

@app.route("/types/")
def types_list():
    """
        Display a list of service request types.
    """
    return render_template('types.html')

@app.route("/daily/")
def daily_list():
    return render_template('daily.html')

@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html'), 404
###
# API calls
###
@app.route("/daily_count")
def daily_count():
    """
        Get a daily count of service requests that were opened and closed during a
        range of dates.
        
        The default range is 60 days.
    """
    days = request.args.get("days", 60)
    
    end_date = get_max_date()
    
    start_date = end_date - timedelta(days=int(days)-1)
        
    start_day, end_day = get_formatted_date(start_date, end_date)
        
    open_count_res = query_db("""
        SELECT 
            CAST(DATE(requested_datetime) as text) as date, COUNT(*), status
        FROM sf_requests 
        WHERE DATE(requested_datetime) BETWEEN (%s) AND (%s) AND status='Open'
        GROUP BY date, status
        ORDER BY date ASC
    """, (start_day, end_day))
    
    closed_count_res = query_db("""
        SELECT 
            CAST(DATE(updated_datetime) as text) as date, COUNT(*), status
        FROM sf_requests 
        WHERE DATE(updated_datetime) BETWEEN (%s) AND (%s) AND status='Closed'
        GROUP BY date, status
        ORDER BY date ASC
    """, (start_day, end_day))
    
    return json.dumps(combine_open_closed_counts(open_count_res, closed_count_res, 
                                                 start_date, end_date))
    
@app.route("/daily_count_by_neighborhood")
def daily_count_by_neighborhood():
    """
        Get a daily count of service requests that were opened and closed in a particular
        neighborhood during a range of dates.
        
        The default range is 60 days.
    """
    neighborhood = request.args.get("neighborhood")
    
    if neighborhood:
        days = request.args.get("days", 60)
        
        end_date = get_max_date()
        
        start_date = end_date - timedelta(days=int(days)-1)
            
        start_day, end_day = get_formatted_date(start_date, end_date)
        
        open_count_res = query_db("""
            SELECT 
                CAST(DATE(r.requested_datetime) as text) as date, COUNT(r.*), r.status
            FROM sf_requests as r
            JOIN pn_geoms as p ON ST_INTERSECTS(geom, ST_MakePoint(r.lon,r.lat))
            WHERE p.neighborho=(%s) AND DATE(r.requested_datetime) BETWEEN (%s) AND (%s) AND status='Open'
            GROUP BY date, r.status
            ORDER BY date ASC
        """, (neighborhood, start_day, end_day))
        
        print 'open count', open_count_res
        
        if open_count_res:
            closed_count_res = query_db("""
                SELECT 
                    CAST(DATE(r.updated_datetime) as text) as date, COUNT(r.*), r.status
                FROM sf_requests as r
                JOIN pn_geoms as p ON ST_INTERSECTS(geom, ST_MakePoint(r.lon,r.lat))
                WHERE p.neighborho=(%s) AND DATE(r.updated_datetime) BETWEEN (%s) AND (%s) AND status='Closed'
                GROUP BY date, r.status
                ORDER BY date ASC
            """, (neighborhood, start_day, end_day))
            
            return json.dumps(combine_open_closed_counts(open_count_res, 
                                                         closed_count_res, 
                                                         start_date, end_date))
        else:
            return create_null_response()
    else:
        return create_null_response()

# Handle dates
@app.route("/requests/daily/<start_day>..<end_day>", methods=['POST', 'GET'])
def get_requests_by_date(start_day=None, end_day=None):
    if start_day and end_day:
        print start_day, end_day
        
        start_date = parse_date(start_day)
        end_date = (end_day and parse_date(end_day)) or start_date + ONE_DAY
        
        res = query_db("""
            SELECT 
                service_request_id,status, service_code, 
                CAST(DATE(requested_datetime)AS text) as requested_date,lat, lon 
            FROM sf_requests 
            WHERE requested_datetime BETWEEN (%s) AND (%s) 
            ORDER BY requested_datetime ASC Limit 1000
        """, (start_date, end_date))
        
        return json.dumps(res)
    else:
        return 'none'

def request_display_by_date(type=None, start_day=None, end_day=None):
    limit = request.args.get('limit', 1, type=str)

    if start_day and end_day:
        start_date = parse_date(start_day)
        end_date = (end_day and parse_date(end_day)) or start_date + ONE_DAY

        res = query_db("""
            SELECT 
                status, service_name, service_request_id, 
                CAST(DATE(requested_datetime) AS text), 
                CAST(DATE(updated_datetime) AS text) as updated_datetime, 
                CAST(DATE(expected_datetime) AS text) as expected_datetime, 
                address, lat, lon 
            FROM sf_requests 
            WHERE requested_datetime BETWEEN (%s) AND (%s) 
            ORDER BY requested_datetime ASC Limit 1000
        """, (start_date, end_date))

        requests_json = json.dumps(res)

        if type == 'jsonp':
            return create_jsonp_response(requests_json, 'data')
        else:
            return render_template('requests_range.html', results=requests_json)
    elif start_day:
        start_date = parse_date(start_day)
        end_date = start_date + ONE_DAY

        res = query_db("""
            SELECT 
                status, service_name, service_request_id, 
                CAST(DATE(requested_datetime) AS text), 
                CAST(DATE(updated_datetime) AS text) as updated_datetime, 
                CAST(DATE(expected_datetime) AS text) as expected_datetime, 
                address, lat, lon 
            FROM sf_requests 
            WHERE requested_datetime BETWEEN (%s) AND (%s) 
            ORDER BY requested_datetime ASC Limit 1000
        """, (start_date, end_date))

        requests_json = json.dumps(res)

        if type == 'jsonp':
            return create_jsonp_response(requests_json, 'data')
        else:
            return render_template('requests_range.html', results=requests_json)
    else:
        return create_null_response()

###
# Statistics
###

# Average Response Time

def calculate_avg_resp_time(neighborhood=None):
    """
        Calculate average response time for a neighborhood or the entire city over
        the past 30 days.
    """
    
    end_date = get_max_date()

    start_date = end_date - timedelta(days=int(30))
    
    if neighborhood:
        res = query_db("""
            SELECT
                AVG((EXTRACT(Epoch from r.updated_datetime - r.requested_datetime)/3600)::Integer) AS "avg_response_time"
            FROM sf_requests as r
            JOIN pn_geoms as p
            JOIN pn_geoms as p ON ST_INTERSECTS(geom, ST_MakePoint(r.lon,r.lat))
            WHERE p.neighborho=(%s) AND r.status='Closed' AND r.requested_datetime BETWEEN (%s) and (%s)
        """, (neighborhood,start_date,end_date))
        
        avg_resp_time = json.dumps(res)
        
        if USE_MEMCACHE:
            # Assumes neighborhood names just have a single spaces (%20) between words
            cache.set(neighborhood.replace(" ","-") + '-art', avg_resp_time, timeout=CACHE_TIMEOUT)
    else:
        res = query_db("""
            SELECT
                AVG((EXTRACT(Epoch from updated_datetime - requested_datetime)/3600)::Integer) as "avg_response_time"
            FROM sf_requests
            WHERE status='Closed' AND requested_datetime between (%s) and (%s)
        """, (start_date,end_date))
        
        avg_resp_time = json.dumps(res)
        
        if USE_MEMCACHE:
            cache.set('sanfrancisco-art', avg_resp_time, timeout=CACHE_TIMEOUT)
    
    return avg_resp_time
    
@app.route("/avg_resp_time")
def get_avg_resp_time():
    avg_resp_time = None
    neighborhood = request.args.get('neighborhood', None)
    
    if neighborhood:
        if USE_MEMCACHE:
            avg_resp_time = cache.get(neighborhood.replace(" ","-") + '-art')
        
        if avg_resp_time is None:
            print 'Not cached'
            avg_resp_time = calculate_avg_resp_time(neighborhood)
            
    else:
        if USE_MEMCACHE:
            avg_resp_time = cache.get('sanfrancisco-art')
        
        if avg_resp_time is None:
            print 'Not cached'
            avg_resp_time = calculate_avg_resp_time()
    
    return avg_resp_time
    
### CSV FOR THE DASHBOARD PAGE ###
import csv
from cStringIO import StringIO
def render_csv(rows, columns, format='csv', content_type='text/plain', cors='*'):
    io = StringIO()
    if format == 'tsv':
        dialect = 'excel-tab'
    else:
        dialect = 'excel'
    
    writer = csv.DictWriter(io, columns, dialect=dialect)
    writer.writeheader()
    writer.writerows(rows)
    response = make_response(io.getvalue(), 200)
    response.headers['Content-Type'] = content_type
    if cors is not None:
        response.headers['Access-Control-Allow-Origin'] = cors
    return response

@app.route("/requests/latest", methods=['GET'])
def get_latest_csv():
    days = request.args.get("days", "20")
    if not days.isdigit():
        raise Exception("'days' must be a number")

    res = query_db("""
        SELECT MAX(requested_datetime) AS max_date
        FROM sf_requests
    """)

    end_date = res[0]['max_date']

    start_date = end_date - timedelta(days=int(days))
    
    fmt = '%Y-%m-%d'
    
    return get_requests_by_date_csv('csv', start_date.strftime(fmt), end_date.strftime(fmt))

@app.route("/requests/reqs.<format>", methods=['GET'])
def get_requests_by_date_csv(format=None,start_day=None, end_day=None):
    if start_day is None:
        start_day = request.args.get('start_day', None)
        end_day = request.args.get('end_day', None)
        time_delta = request.args.get('time_delta', None)
    
    if format in ('csv','tsv'):
        if start_day:
            print start_day, end_day
            
            start_date = parse_date(start_day)
            end_date = (end_day and parse_date(end_day)) or start_date + ONE_DAY
        elif end_day:
            end_date = parse_date(end_day)
            start_date = end_date - timedelta(days=int(time_delta))
            
        res = query_db("""
            SELECT 
                service_request_id,status,service_code, 
                CAST(DATE(requested_datetime)AS text) as requested_date,lat,lon,neighborhood
            FROM sf_requests 
            WHERE DATE(requested_datetime) BETWEEN (%s) AND (%s) 
            ORDER BY requested_datetime DESC Limit 6000
        """, (start_date, end_date))
        
        # TODO: Only return requests under the specific categories
    
        return render_csv(res, ['service_request_id', 'status','service_code', 'requested_date','lat','lon','neighborhood'])
    else:
        return create_null_response()
        
@app.route("/requests/neighborhood/latest", methods=['GET'])
def get_latest_neighborhood_csv(neighborhood=None):
    days = request.args.get("days", "60")
    neighborhood = request.args.get("neighborhood", None)
    if not days.isdigit():
        raise Exception("'days' must be a number")

    res = query_db("""
        SELECT MAX(requested_datetime) AS max_date
        FROM sf_requests
    """)

    end_date = res[0]['max_date']

    start_date = end_date - timedelta(days=int(days))
    
    fmt = '%Y-%m-%d'
    
    return get_requests_by_neighborhood_date_csv('csv',neighborhood,start_date.strftime(fmt), end_date.strftime(fmt))

@app.route("/requests/neighborhoods/reqs.<format>", methods=['GET'])
def get_requests_by_neighborhood_date_csv(format=None,neighborhood=None,start_day=None, end_day=None):
    # Needs to account for updated datetime
    
    if start_day is None:
        start_day = request.args.get('start_day', None)
        end_day = request.args.get('end_day', None)
        neighborhood = request.args.get('neighborhood', None)
        time_delta = request.args.get('time_delta', None)
        
    neighborhoods = {'bayview':'Bayview',
                     'bernal':'Bernal Heights', 
                     'castro':'Castro/Upper Market',
                     'chinatown':'Chinatown',
                     'crocker_amazon':'Crocker Amazon', 
                     'diamond_heights':'Diamond Heights', 
                     'downtown':'Downtown/Civic Center', 
                     'excelsior':'Excelsior', 
                     'financial_district':'Financial District', 
                     'glen_park':'Glen Park', 
                     'gg_park':'Golden Gate Park', 
                     'haight_ashbury':'Haight Ashbury',
                     'inner_richmond': 'Inner Richmond', 
                     'inner_sunset':'Inner Sunset', 
                     'lakeshore':'Lakeshore', 
                     'marina':'Marina', 
                     'mission':'Mission',
                     'nob_hill':'Nob Hill', 
                     'noe_valley':'Noe Valley', 
                     'north_beach':'North Beach', 
                     'ocean_view':'Ocean View',
                     'outer_mission':'Outer Mission', 
                     'outer_richmond':'Outer Richmond', 
                     'outer_sunset':'Outer Sunset', 
                     'pacific_heights':'Pacific Heights',
                     'parkside':'Parkside', 
                     'potrero_hill':'Potrero Hill', 
                     'presidio':'Presidio', 
                     'presidio_heights':'Presidio Heights',
                     'russian_hill':'Russian Hill', 
                     'seacliff':'Seacliff', 
                     'soma':'South of Market', 
                     'ti':'Treasure Island/YBI', 
                     'twin_peaks':'Twin Peaks',
                     'visitacion':'Visitacion Valley', 
                     'west_twin_peaks':'West of Twin Peaks', 
                     'western_addition':'Western Addition'}
    
    if format in ('csv','tsv'):
        if start_day:
            print start_day, end_day
            
            start_date = parse_date(start_day)
            end_date = (end_day and parse_date(end_day)) or start_date + ONE_DAY
        elif end_day:
            end_date = parse_date(end_day)
            start_date = end_date - timedelta(days=int(time_delta))
            
        res = query_db("""
            SELECT 
                service_request_id,status,service_code, 
                CAST(DATE(requested_datetime)AS text) as requested_date,lat,lon,neighborhood
            FROM sf_requests 
            WHERE DATE(requested_datetime) BETWEEN (%s) AND (%s) and neighborhood=(%s)
            ORDER BY requested_datetime DESC Limit 6000
        """, (start_date, end_date, neighborhood))
        
        # TODO: Only return requests under the specific categories
    
        return render_csv(res, ['service_request_id', 'status','service_code', 'requested_date','lat','lon','neighborhood'])
    else:
        return create_null_response()

@app.route("/stats/sr_counts", methods=['GET'])
def get_sr_counts_by_range():        
    days = request.args.get("days", "60")
    neighborhood = request.args.get("neighborhood", None)
    
    res = query_db("""
        SELECT MAX(requested_datetime) AS max_date
        FROM sf_requests
    """)
    
    end_date = res[0]['max_date']

    start_date = end_date - timedelta(days=int(days))
    
    fmt = '%Y-%m-%d'
    
    start_day = start_date.strftime(fmt);
    end_day = end_date.strftime(fmt)
    
    if neighborhood:
        # TODO: Service request counts per neighborhood
        pass
    else:
        res = query_db("""
            SELECT 
                CAST(DATE(requested_datetime)AS text) as r_dt, service_code, COUNT(service_code) AS count 
            FROM sf_requests 
            WHERE DATE(requested_datetime) BETWEEN (%s) AND (%s) 
            GROUP by r_dt,service_code
            ORDER by r_dt DESC
        """, (start_date, end_date))
        
        data = {}
        
        service_list = {"1":"Garbage","2":"Sidewalk or Street","3":"Sidewalk or Street",
        "4":"Garbage","5":"Sidewalk or Street","6":"Trees","7":"Defacement / Graffiti",
        "8":"Defacement / Graffiti","9":"Defacement / Graffiti","10":"Defacement / Graffiti",
        "11":"Defacement / Graffiti","12":"Defacement / Graffiti","13":"Defacement / Graffiti",
        "15":"Garbage","16":"Trees","17":"Sidewalk or Street","18":"Sidewalk or Street",
        "19":"Garbage","20":"Trees","21":"Sidewalk or Street","22":"Sewage","23":"Sewage",
        "24":"Sidewalk or Street","25":"Sidewalk or Street","26":"Sidewalk or Street","27":"Trees",
        "29":"Defacement / Graffiti","30":"Sewage","31":"Sewage","32":"Sewage","33":"Vehicles",
        "47":"Water","68":"Defacement / Graffiti","172":"Garbage","174":"Garbage","307":"Sewage","375":"Sidewalk or Street",
        "376":"Sidewalk or Street","377":"Sidewalk or Street","378":"Sidewalk or Street","379":"Sidewalk or Street"}
        
        counts = {}
        
        for i, sr_date_count in enumerate(res):
            if sr_date_count['service_code'] in service_list:
                if sr_date_count['r_dt'] in data:
                    current_category = service_list[sr_date_count['service_code']]
                    
                    if current_category in counts:
                        counts[current_category] += int(sr_date_count['count'])
                    else:
                        counts[current_category] = int(sr_date_count['count'])
                
                else:
                    data[sr_date_count['r_dt']] = {}
                        
                    current_category = service_list[sr_date_count['service_code']]
                    
                    if current_category in counts:
                        counts[current_category] += int(sr_date_count['count'])
                    else:
                        counts[current_category] = int(sr_date_count['count'])
            
                data[sr_date_count['r_dt']] = counts
            
            if i < len(res) - 1: 
                if res[i]['r_dt'] != res[i+1]['r_dt']:
                    counts = {}
            
        return json.dumps(data)

@app.route("/stats/neighborhood_sc_counts", methods=['GET'])
def get_neighborhood_sc_counts_by_range():
    days = request.args.get("days", "60")
    
    res = query_db("""
        SELECT MAX(requested_datetime) AS max_date
        FROM sf_requests
    """)
    
    end_date = res[0]['max_date']
    
    start_date = end_date - timedelta(days=int(days))
    
    fmt = '%Y-%m-%d'
    
    start_day = start_date.strftime(fmt);
    end_day = end_date.strftime(fmt)
        
    results = query_db("""
        SELECT
            p.neighborho as neigh, CAST(DATE(r.requested_datetime) as text) as r_dt, 
            r.service_code as sc, COUNT(r.service_code) as count
        FROM sf_requests as r
        JOIN pn_geoms as p ON ST_INTERSECTS(geom, ST_MakePoint(r.lon,r.lat))
        WHERE DATE(r.requested_datetime) between (%s) and (%s) 
        GROUP by neigh, r_dt, sc
        ORDER by r_dt DESC
    """, (start_date, end_date))
    
    neighborhoods = {'Bayview':{}, 'Bernal Heights':{}, 'Castro/Upper Market':{}, 'Chinatown':{}, 
    'Crocker Amazon':{}, 'Diamond Heights':{}, 'Downtown/Civic Center':{}, 'Excelsior':{}, 
    'Financial District':{}, 'Glen Park':{}, 'Golden Gate Park':{}, 'Haight Ashbury':{}, 'Inner Richmond':{}, 
    'Inner Sunset':{}, 'Lakeshore':{}, 'Marina':{}, 'Mission':{}, 'Nob Hill':{}, 'Noe Valley':{}, 'North Beach':{}, 
    'Ocean View':{}, 'Outer Mission':{}, 'Outer Richmond':{}, 'Outer Sunset':{}, 'Pacific Heights':{}, 'Parkside':{}, 
    'Potrero Hill':{}, 'Presidio':{}, 'Presidio Heights':{}, 'Russian Hill':{}, 'Seacliff':{}, 'South of Market':{}, 
    'Treasure Island/YBI':{}, 'Twin Peaks':{}, 'Visitacion Valley':{}, 'West of Twin Peaks':{}, 'Western Addition':{}}
    
    service_list = {"1":"Garbage","2":"Sidewalk or Street","3":"Sidewalk or Street",
        "4":"Garbage","5":"Sidewalk or Street","6":"Trees","7":"Defacement / Graffiti",
        "8":"Defacement / Graffiti","9":"Defacement / Graffiti","10":"Defacement / Graffiti",
        "11":"Defacement / Graffiti","12":"Defacement / Graffiti","13":"Defacement / Graffiti",
        "15":"Garbage","16":"Trees","17":"Sidewalk or Street","18":"Sidewalk or Street",
        "19":"Garbage","20":"Trees","21":"Sidewalk or Street","22":"Sewage","23":"Sewage",
        "24":"Sidewalk or Street","25":"Sidewalk or Street","26":"Sidewalk or Street","27":"Trees",
        "29":"Defacement / Graffiti","30":"Sewage","31":"Sewage","32":"Sewage","33":"Vehicles",
        "47":"Water","68":"Defacement / Graffiti","172":"Garbage","174":"Garbage","307":"Sewage","375":"Sidewalk or Street",
        "376":"Sidewalk or Street","377":"Sidewalk or Street","378":"Sidewalk or Street","379":"Sidewalk or Street"}
    
    data = {}
    
    for result in results:
        if result['r_dt'] not in data:
            data[result['r_dt']] = {}
        
        if result['sc'] in service_list:
            if result['neigh'] not in data[result['r_dt']]:
                data[result['r_dt']][result['neigh']] = {}
            
            if service_list[result['sc']] in data[result['r_dt']][result['neigh']]:
                data[result['r_dt']][result['neigh']][service_list[result['sc']]] = result['count'] + data[result['r_dt']][result['neigh']][service_list[result['sc']]]
            else:
                data[result['r_dt']][result['neigh']][service_list[result['sc']]] = result['count']

    return json.dumps(data)

@app.route("/stats/neighborhood_counts", methods=['GET'])
def get_neighborhood_counts_by_range():
    days = request.args.get("days", "60")
    neighborhood = request.args.get("neighborhood", None)
    
    res = query_db("""
        SELECT MAX(requested_datetime) AS max_date
        FROM sf_requests
    """)
    
    end_date = res[0]['max_date']

    start_date = end_date - timedelta(days=int(days))
    
    fmt = '%Y-%m-%d'
    
    start_day = start_date.strftime(fmt);
    end_day = end_date.strftime(fmt)
    
    
    results = query_db("""
        SELECT 
            p.neighborho as neigh, CAST(DATE(r.requested_datetime)AS text) as r_dt, COUNT(r.service_code) AS count 
        FROM sf_requests as r
        JOIN pn_geoms as p ON ST_INTERSECTS(geom, ST_MakePoint(r.lon,r.lat))
        WHERE DATE(r.requested_datetime) BETWEEN (%s) AND (%s) 
        GROUP by neigh, r_dt
        ORDER by r_dt DESC
    """, (start_date, end_date))
    
    data = {}
    
    for result in results:
        if result['r_dt'] not in data:
            #day_data[result['neigh']] = result['count'];
            data[result['r_dt']] = {result['neigh']:result['count']}
        else:
            #day_data[result['neigh']] = result['count']
            data[result['r_dt']][result['neigh']] = result['count']
            #data[result['r_dt']] = day_data
    
    return json.dumps(data)

@app.route("/stats/category_counts", methods=['GET'])
def get_category_counts_by_period():
    """
        Get category counts for a specified time delta
    """
    days = request.args.get("days", "30")
    neighborhood = request.args.get("neighborhood", None)
    
    res = query_db("""
        SELECT MAX(requested_datetime) AS max_date
        FROM sf_requests
    """)
    
    end_date = res[0]['max_date']

    start_date = end_date - timedelta(days=int(days))
    
    if neighborhood:
        results = query_db("""
            SELECT 
                r.category, COUNT(r.category) as count
            FROM sf_requests as r
            JOIN pn_geoms as p ON ST_INTERSECTS(geom, ST_MakePoint(r.lon,r.lat))
            WHERE DATE(r.requested_datetime) BETWEEN (%s) and (%s) and p.neighborho=(%s)
            GROUP BY category;
        """, (start_date,end_date,neighborhood)) 
    else:
        results = query_db("""
            SELECT 
                category, COUNT(category) as count
            FROM sf_requests
            WHERE DATE(requested_datetime) BETWEEN (%s) and (%s)
            GROUP BY category;
        """, (start_date,end_date))    
    
    return json.dumps(results)

if __name__ == "__main__":
    import optparse
    parser = optparse.OptionParser()
    parser.add_option('--port', dest='port', type='int', default=80)
    parser.add_option('--host', dest='host', default="0.0.0.0")
    options, args = parser.parse_args()
    
    app.run(host=options.host, port=options.port)
