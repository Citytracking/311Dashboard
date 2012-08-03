import psycopg2

import psycopg2.extras

from flask import Flask, g
from flask import render_template, Response
from flask import request, session, redirect, url_for, abort, flash, json, jsonify, make_response

from datetime import date, datetime, timedelta
TODAY = date.today()
ONE_DAY = timedelta(days=1)

# configuration
DATABASE = 'data/requests.db'
SECRET_KEY = 'aaa' # change later
DEBUG = True # delete this later, TURN OFF DEBUGGGING LATER

app = Flask(__name__)
app.config.from_object(__name__)

def connect_db():
    return psycopg2.connect("host=localhost password=77 dbname=sf_311 user=sf_311")

@app.before_request
def before_request():
    g.db = connect_db()

@app.teardown_request
def teardown_request(exception):
        if hasattr(g, 'db'):
            g.db.close()

def parse_date(date_str, date_format='%Y-%m-%d'):
    try:
        return datetime.strptime(date_str, date_format)
    except Exception, e:
        return None

def query_db(query, args=()):
        #dbutils pylons
        cur = g.db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        try:
                cur.execute(query, args)

                query_str = cur.mogrify(query,args)
                print query_str
        except psycopg2.DataError,err:
                print 'Error', err

        print 'description', cur.description
        
        try:
                res = cur.fetchall() # fetching everything, rename the method?
        except psycopg2.ProgrammingError, err:
                res = None
                
                print 'Error', err

        cur.close() # is this necessary?

        return res

def create_json(attrs, res):
        # stats

        # requests.append({'stats': {'count': len(res)}})

        requests = []

        for row in res:
            # Check if date day is different? Just calculate stats in python?
            # http://stackoverflow.com/questions/51553/why-are-sql-aggregate-functions-so-much-slower-than-python-and-java-or-poor-man
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

@app.route("/requests/requests.<type>", methods=['POST', 'GET'])
@app.route("/requests/<service_request_id>/requests.<type>", methods=['POST', 'GET'])
@app.route("/requests/status/<status>/requests.<type>", methods=['POST', 'GET'])
def request_display(type=None, service_request_id=None, status=None):
    limit = request.args.get('limit', 10, type=str)

    # Need to add things like date ascending
    # add callback parameter

    #http://127.0.0.1:5000/requests/status/Closed/requests.jsonp?limit=50
    # API key? hide endpoint? http://jetfar.com/simple-api-key-generation-in-python/
    #Only use post if we end up with very long url names

    # lowercase at some point
    # get the official request list

    # Put in paramater for number of results
    
    # Cache the json generated on each call? otherwise call the database.

    if type == 'jsonp':
            # cursor_factory=psycopg2.extras.DictCursor What does this do?

            if status:
                    if status == 'Open' or status == 'Closed':
                        # Handle uppercase? Make it case insensitive. Private API.
                        # change postgres to stop disitinguishing between upper and lower case
                        #cur.execute("""SELECT status, service_name, service_request_id, CAST(requested_datetime AS text), address, lat, lon FROM sf_requests WHERE status=(%s) ORDER BY requested_datetime DESC Limit (%s)""", (status, limit))
                        
                        results = query_db("""SELECT status, service_name, service_request_id, CAST(requested_datetime AS text), address, lat, lon FROM sf_requests WHERE status=(%s) ORDER BY requested_datetime DESC Limit (%s)""", (status, limit))
            
            elif service_request_id:
                    #http://127.0.0.1:5000/requests/1134232/requests.jsonp

                    results = query_db("""SELECT status, service_name, service_request_id, CAST(requested_datetime AS text), address, lat, lon FROM sf_requests WHERE service_request_id=(%s) Limit (%s)""", (service_request_id, limit))
                    
            else:
                    results = query_db("""SELECT status, service_name, service_request_id, CAST(requested_datetime AS text), address, lat, lon FROM sf_requests ORDER BY requested_datetime DESC Limit (%s)""", (limit,))
            

            return create_jsonp_response_from_dbresult(results)
    else:
            abort(404)

@app.route("/requests/stats/<start_day>..<end_day>/stats.<type>", methods=['POST', 'GET'])
def stats(type=None, start_day=None, end_day=None):
    if type == 'jsonp': # just have type json with jsonp?
        #for k in psycopg2.extensions.string_types.keys():
            #del psycopg2.extensions.string_types[k]

        #psycopg2.extensions.string_types.clear()

        if start_day:
            start_date = parse_date(start_day)
            end_date = (end_day and parse_date(end_day)) or start_date + ONE_DAY

            results = query_db("""SELECT CAST(DATE(requested_datetime) AS text) AS date, COUNT(*) AS reports FROM sf_requests WHERE requested_datetime BETWEEN (%s) AND (%s) GROUP BY date ORDER BY date ASC""", (start_date, end_date))

            # stringify
            #for result in results:
                #result['date'] = str(result['date'])

            if results:
                requests_json = json.dumps(results)

                return create_jsonp_response(requests_json, 'data')
            else:
                return create_null_response()

@app.route("/neighborhoods/data/<neighborhood>.<type>", methods=['POST', 'GET'])
@app.route("/neighborhoods/<neighborhood>", methods=['POST', 'GET'])
def neighborhoods(type=None, neighborhood=None, count=None, daily_count=None, top_requests=None, weekly_count=None):
    # need to change neighborhood names in database
    if neighborhood:
        if neighborhood == 'Downtown_Civic_Center':
            neighborhood = 'Downtown/Civic Center'
        elif neighborhood == 'Castro_Upper_Market':
            neighborhood = 'Castro/Upper Market'
        else:
            neighborhood = neighborhood.replace('_',' ');
    
        results = query_db("""select r.status,r.service_code,CAST(DATE(r.requested_datetime) AS text), CAST(DATE(r.updated_datetime) AS text) as updated_datetime, CAST(DATE(r.expected_datetime) AS text) as expected_datetime, r.service_name,r.lon,r.lat,p.neighborho from sf_requests as r join pn_geoms as p on ST_INTERSECTS(geom, ST_GeomFromText('POINT(' || r.lon || ' ' || r.lat || ')')) WHERE p.neighborho=(%s) LIMIT 1000""", (neighborhood,))
        
        count = query_db("""select count(r.*), r.status, p.neighborho from sf_requests as r join pn_geoms as p on ST_INTERSECTS(geom, ST_GeomFromText('POINT(' || r.lon || ' ' || r.lat || ')')) WHERE p.neighborho=(%s) AND r.requested_datetime between now() - INTERVAL '30 DAY' and now() group by r.status, p.neighborho""", (neighborhood,))
        #print 'count', count
        # [{'count': 202L, 'neighborho': 'Seacliff'}]
        # count [{'count': 64L, 'status': 'Open', 'neighborho': 'West of Twin Peaks'}, {'count': 60L, 'status': 'Closed', 'neighborho': 'West of Twin Peaks'}]

        daily_count_res = query_db("""SELECT CAST(DATE(r.requested_datetime) as text), COUNT(r.*) FROM sf_requests as r join pn_geoms as p on ST_INTERSECTS(geom, ST_GeomFromText('POINT(' || r.lon || ' ' || r.lat || ')')) WHERE p.neighborho=(%s) AND r.requested_datetime BETWEEN NOW() - INTERVAL '12 Week' AND NOW() GROUP BY DATE(r.requested_datetime) ORDER BY DATE(r.requested_datetime) ASC""", (neighborhood,))
        print 'daily_count_res', daily_count_res

        weekly_count_res = query_db("""SELECT CAST(DATE(date_trunc('week', r.requested_datetime)) as text) as "Week", COUNT(r.*) FROM sf_requests as r join pn_geoms as p on ST_INTERSECTS(geom, ST_GeomFromText('POINT(' || r.lon || ' ' || r.lat || ')')) WHERE p.neighborho=(%s) AND r.requested_datetime BETWEEN NOW() - INTERVAL '3 months' AND NOW() GROUP BY "Week" ORDER BY "Week" ASC""", (neighborhood,))

        top_requests = query_db("""select count(r.*), r.service_name, r.service_code, p.neighborho from sf_requests as r join pn_geoms as p on ST_INTERSECTS(geom, ST_GeomFromText('POINT(' || r.lon || ' ' || r.lat || ')')) WHERE p.neighborho=(%s) AND r.requested_datetime between now() - INTERVAL '30 DAY' and now() group by r.service_name, r.service_code, p.neighborho order by count(r.*) DESC;""", (neighborhood,))

        if results:
            requests_json = json.dumps(results)
            
            if type == 'jsonp':
                return create_jsonp_response(requests_json, 'data')
            else:
                res = requests_json
                return render_template('neighborhood.html', neighborhood=neighborhood, results=res, count=count, daily_count=daily_count_res, weekly_count=weekly_count_res, top_requests=top_requests)
        else:
            return create_null_response()

@app.route("/types/<service_code>", methods=['POST', 'GET'])
def types(type=None, service_code=None, count=None, total_count=None, weekly_count=None):
    # strip leading zeros from service json

    if service_code:
        results = query_db("""SELECT status, service_name, service_request_id, CAST(DATE(requested_datetime) AS text), CAST(DATE(updated_datetime) AS text) as updated_datetime, CAST(DATE(expected_datetime) AS text) as expected_datetime, address, lat, lon FROM sf_requests WHERE service_code=(%s) ORDER BY requested_datetime ASC Limit 1000""", (service_code.lstrip('0'),))

        total_count_per_day = query_db("""SELECT CAST(DATE(requested_datetime) as text) as date, COUNT(*) as count FROM sf_requests WHERE service_code=(%s) AND requested_datetime BETWEEN NOW() - INTERVAL '30 DAY' AND NOW() GROUP BY date ORDER BY count DESC""", (service_code.lstrip('0'),))

        count = query_db("""SELECT status, count(*) FROM sf_requests WHERE service_code=(%s) and requested_datetime between now() - INTERVAL '30 DAY' and now() group by status""", (service_code.lstrip('0'),))

        #print 'count', count

        # count [{'status': 'Closed', 'count': 9L}, {'status': 'Open', 'count': 60L}]

        daily_count_res = query_db("""SELECT CAST(DATE(requested_datetime) as text), COUNT(*), status FROM sf_requests WHERE service_code=(%s) AND requested_datetime BETWEEN NOW() - INTERVAL '60 DAY' AND NOW() GROUP BY DATE(requested_datetime), status ORDER BY DATE(requested_datetime) ASC""", (service_code.lstrip('0'),))
        
        weekly_count_res = query_db("""SELECT CAST(DATE(date_trunc('week', requested_datetime)) as text) as "Week", COUNT(*) FROM sf_requests WHERE service_code=(%s) AND requested_datetime BETWEEN NOW() - INTERVAL '3 months' AND NOW() GROUP BY "Week" ORDER BY "Week" ASC""", (service_code.lstrip('0'),))

        daily_count_list = []

        for i,j in enumerate(daily_count_res):
            if (i+1) == len(daily_count_res):
                break
            # can consolidate these into one statement
            elif daily_count_res[i]['date'] == daily_count_res[i+1]['date']:
                #daily_count_list.append({str(int(.5*i)): {daily_count_res[i]['date']: {daily_count_res[i]['status']:daily_count_res[i]['count'], daily_count_res[i+1]['status']: daily_count_res[i+1]['count']}}})
                daily_count_list.append({'date': daily_count_res[i]['date'], daily_count_res[i]['status']:daily_count_res[i]['count'], daily_count_res[i+1]['status']: daily_count_res[i+1]['count']})
            elif daily_count_res[i-1]['date'] != daily_count_res[i]['date']:
                daily_count_list.append({'date': daily_count_res[i]['date'], daily_count_res[i]['status']:daily_count_res[i]['count']})


        #print daily_count_list

        daily_count_json = json.dumps(daily_count_list)

        requests_json = json.dumps(results)

        count = json.dumps(count)
        
        if type == 'jsonp':
            return create_jsonp_response(requests_json, 'data')
        else:
            res = requests_json
            return render_template('type.html', service_code=service_code, results=res, count=count, daily_count=daily_count_json, total_count=total_count_per_day, weekly_count=weekly_count_res)
    else:
        return create_null_response()

@app.route("/daily_count")
def daily_count(count=None):
    days = request.args.get("days", 60)
    
    daily_count_res = query_db("""
        SELECT 
            CAST(DATE(requested_datetime) as text), COUNT(*), status 
        FROM sf_requests 
        WHERE requested_datetime BETWEEN NOW() - INTERVAL '(%s) DAY' AND NOW() 
        GROUP BY DATE(requested_datetime), status 
        ORDER BY DATE(requested_datetime) ASC
    """, (days,))

    #print daily_count_res

    daily_count_list = []

    for i,j in enumerate(daily_count_res):
        if (i+1) == len(daily_count_res):
            break
        elif daily_count_res[i]['date'] == daily_count_res[i+1]['date']:
            #daily_count_list.append({str(int(.5*i)): {daily_count_res[i]['date']: {daily_count_res[i]['status']:daily_count_res[i]['count'], daily_count_res[i+1]['status']: daily_count_res[i+1]['count']}}})
            daily_count_list.append({'date': daily_count_res[i]['date'], daily_count_res[i]['status']:daily_count_res[i]['count'], daily_count_res[i+1]['status']: daily_count_res[i+1]['count']})
        else:
            continue

    #print daily_count_list

    return json.dumps(daily_count_list)
    
@app.route("/daily_count_by_neighborhood")
def daily_count_by_neighborhood(count=None):
    days = request.args.get("days", 60)
    neighborhood = request.args.get("neighborhood")
    
    end_date = datetime.now()
    
    start_date = end_date - timedelta(days=int(days))
    
    fmt_start_date = datetime.strftime(start_date, '%Y-%m-%d')
    
    fmt_end_date = datetime.strftime(end_date, '%Y-%m-%d')
        
    daily_count_res = query_db("""
        SELECT 
            CAST(DATE(r.requested_datetime) as text), count(r.*), r.status
        FROM sf_requests as r
        JOIN pn_geoms as p
        ON ST_INTERSECTS(geom, ST_GeomFromText('POINT(' || r.lon || ' ' || r.lat || ')'))
        WHERE p.neighborho=(%s) AND requested_datetime BETWEEN DATE((%s)) AND DATE((%s))
        GROUP BY date(r.requested_datetime), r.status order by date(r.requested_datetime) ASC
    """, (neighborhood, fmt_start_date, fmt_end_date))
    
    print daily_count_res
    
    input_exhausted = False
    
    date = start_date
    
    count = 0
    
    input = daily_count_res[count]
    
    next_date = start_date
    
    results = []
    
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
            
            if count >= len(daily_count_res):
                input_exhausted = True
            else:
                input = daily_count_res[count]
        
        date = date + timedelta(1)
        
        results.append(info)
    """
    for d in daily_count_res:
        if d['date'] == fmt_start_date:
            print 'found date'
    """
    
    return json.dumps(results)
    
@app.route("/")
def home(daily_count=None):
    daily_count_res = query_db("""
        SELECT 
            CAST(DATE(requested_datetime) as text), COUNT(*), status 
        FROM sf_requests 
        WHERE requested_datetime BETWEEN NOW() - INTERVAL '60 DAY' AND NOW() 
        GROUP BY DATE(requested_datetime), status 
        ORDER BY DATE(requested_datetime) ASC
    """)

    #print daily_count_res

    daily_count_list = []

    for i,j in enumerate(daily_count_res):
        if (i+1) == len(daily_count_res):
            break
        elif daily_count_res[i]['date'] == daily_count_res[i+1]['date']:
            #daily_count_list.append({str(int(.5*i)): {daily_count_res[i]['date']: {daily_count_res[i]['status']:daily_count_res[i]['count'], daily_count_res[i+1]['status']: daily_count_res[i+1]['count']}}})
            daily_count_list.append({'date': daily_count_res[i]['date'], daily_count_res[i]['status']:daily_count_res[i]['count'], daily_count_res[i+1]['status']: daily_count_res[i+1]['count']})
        else:
            continue

    print daily_count_list

    daily_count_json = json.dumps(daily_count_list)

    #print daily_count_json

    return render_template('home.html', daily_count=daily_count_json)

@app.route("/neighborhoods/")
def neighborhoods_list():
    results = query_db("""SELECT service_name, service_code, count(*) as count FROM sf_requests WHERE requested_datetime between NOW() - INTERVAL '3 MONTH' AND NOW() GROUP BY service_name, service_code order by count DESC Limit 15""")

    top_neighborhoods = query_db("""select count(r.*) as count,p.neighborho from sf_requests as r join pn_geoms as p on ST_INTERSECTS(geom, ST_GeomFromText('POINT(' || r.lon || ' ' || r.lat || ')')) WHERE r.requested_datetime between now() - INTERVAL '30 DAY' and now() group by p.neighborho order by count desc""")

    if results:
        requests_json = json.dumps(results)

    res = json.dumps(results)

    return render_template('neighborhoods.html', results=res, top_neighborhoods=top_neighborhoods)

@app.route("/types/")
def types_list():
    return render_template('types.html')

@app.route("/daily/")
def daily_list():
    return render_template('daily.html')
    

@app.route("/requests/latest", methods=['POST', 'GET'])
def get_latest_requests():
    days = request.args.get("days", "60")
    if not days.isdigit():
        raise Exception("'days' must be a number")

    res = query_db("""
        SELECT MAX(requested_datetime) AS max_date
        FROM sf_requests
    """)
    # print 'latest:', res
    end_date = res[0]['max_date']
    # print 'max date:', end_date

    start_date = end_date - timedelta(days=int(days))
    fmt = '%Y-%m-%d'
    return get_requests_by_date(start_date.strftime(fmt), end_date.strftime(fmt))

# Handle dates
@app.route("/requests/daily/<start_day>..<end_day>", methods=['POST', 'GET'])
def get_requests_by_date(start_day=None, end_day=None):
    print 'get requests'
    if start_day and end_day:
        print start_day, end_day
        
        start_date = parse_date(start_day)
        end_date = (end_day and parse_date(end_day)) or start_date + ONE_DAY
        
        res = query_db("""
            SELECT 
                status, service_name, service_request_id, 
                CAST(DATE(requested_datetime)AS text) as requested_date, 
                CAST(DATE(updated_datetime) AS text) as updated_date, 
                CAST(DATE(expected_datetime) AS text) as expected_date, address, 
                lat, lon 
            FROM sf_requests 
            WHERE requested_datetime BETWEEN (%s) AND (%s) 
            ORDER BY requested_datetime ASC Limit 1000
        """, (start_date, end_date))
        return json.dumps(res)
    else:
        return 'none'

# Handle dates
"""
@app.route("/requests/daily/<start_day>..<end_day>/requests.<type>", methods=['POST', 'GET'])
@app.route("/daily/<start_day>..<end_day>", methods=['POST', 'GET'])
@app.route("/requests/daily/<start_day>/requests.<type>", methods=['POST', 'GET'])
@app.route("/daily/<start_day>", methods=['POST', 'GET'])
"""
def request_display_by_date(type=None, start_day=None, end_day=None):
    limit = request.args.get('limit', 1, type=str) # don't repeat?
    # just try count and group by and see how slow it is
    #limit = 10

    # ideal structure is {some-date: [incident, incident, incident]}

    if start_day and end_day:
        start_date = parse_date(start_day)
        end_date = (end_day and parse_date(end_day)) or start_date + ONE_DAY

        res = query_db("""SELECT status, service_name, service_request_id, CAST(DATE(requested_datetime) AS text), CAST(DATE(updated_datetime) AS text) as updated_datetime, CAST(DATE(expected_datetime) AS text) as expected_datetime, address, lat, lon FROM sf_requests WHERE requested_datetime BETWEEN (%s) AND (%s) ORDER BY requested_datetime ASC Limit 1000""", (start_date, end_date))

        requests_json = json.dumps(res)

        print 'requests json', requests_json

        if type == 'jsonp':
            return create_jsonp_response(requests_json, 'data')
        else:
            return render_template('requests_range.html', results=requests_json)
    elif start_day:
        # can consolidate this code

        start_date = parse_date(start_day)
        #end_date = (end_day and parse_date(end_day)) or start_date + ONE_DAY
        end_date = start_date + ONE_DAY

        res = query_db("""SELECT status, service_name, service_request_id, CAST(DATE(requested_datetime) AS text), CAST(DATE(updated_datetime) AS text) as updated_datetime, CAST(DATE(expected_datetime) AS text) as expected_datetime, address, lat, lon FROM sf_requests WHERE requested_datetime BETWEEN (%s) AND (%s) ORDER BY requested_datetime ASC Limit 1000""", (start_date, end_date))

        requests_json = json.dumps(res)

        print 'requests json', requests_json

        if type == 'jsonp':
            return create_jsonp_response(requests_json, 'data')
        else:
            return render_template('requests_range.html', results=requests_json)
    else:
        return create_null_response()
        
@app.route("/avg_resp_time")
def avg_resp_time():
    neighborhood = request.args.get('neighborhood', None)
    
    if neighborhood:
        res = query_db("""
            SELECT
                AVG((EXTRACT(Epoch from r.updated_datetime - r.requested_datetime)/3600)::Integer) AS "avg_response_time"
            FROM sf_requests as r
            JOIN pn_geoms as p
            ON ST_INTERSECTS(geom, ST_GeomFromText('POINT(' || r.lon || ' ' || r.lat || ')'))
            WHERE p.neighborho=(%s) AND r.status='Closed' AND r.requested_datetime BETWEEN now() - INTERVAL '7 day' AND now()
        """, (neighborhood,))
                
        return json.dumps(res)
    else:
        res = query_db("""
            SELECT
                AVG((EXTRACT(Epoch from updated_datetime - requested_datetime)/3600)::Integer) as "avg_response_time"
            FROM sf_requests
            WHERE status='Closed' AND requested_datetime between now() - interval '7 day' and now()
        """)
        
        return json.dumps(res)
                    
@app.route("/dashboard/")
def dashboard():
    return render_template('dashboard.html')

@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html'), 404

if __name__ == "__main__":
    import optparse
    parser = optparse.OptionParser()
    parser.add_option('--port', dest='port', type='int', default=80)
    parser.add_option('--host', dest='host', default="0.0.0.0")
    options, args = parser.parse_args()


    #app.run()
    app.run(host=options.host, port=options.port)
