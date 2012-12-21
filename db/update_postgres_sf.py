import sys
import datetime
import urllib
# from xml import dom, parsers
from xml.dom import minidom
import xml.parsers.expat
import psycopg2
import json

days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

ONE_DAY = datetime.timedelta(1)

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def load_config(filename):
    with open(filename, 'rt') as f:
        config = json.load(f)
        
    return config

def append_log(file_name, message):
    with open(file_name, 'a') as log_file:
        log_file.write('\n') 
	log_file.write(message)
    
def compute_time_range(end_date=None, num_of_days=1):
    """Computing the the start and end date for our Open311 query"""

    days_delta = datetime.timedelta(days=num_of_days)

    if end_date is None:
        end_date = datetime.datetime.utcnow() - datetime.timedelta(days=1)

    #end = end_date.replace(hour=0, minute=0, second=0, microsecond=0) + ONE_DAY # Making sure we get everything
    end = end_date.replace(hour=0, minute=0, second=0, microsecond=0)

    start = end - days_delta

    return (start,end)

def get_requests(start, end):
    """Retrieving service request data from the Open311 API"""

    #base_url = r'https://open311.sfgov.org/Open311/v2/requests.xml'
    base_url = config['base_url']

    query_args = {
                  'jurisdiction_id' : 'sfgov.org', 
                  'start_date' : start.isoformat() +'Z', 
                  'end_date' : end.isoformat() + 'Z'
                 }

    encoded_args = urllib.urlencode(query_args)

    data_url = base_url + '?' + encoded_args
    print data_url

    try:
        response = urllib.urlopen(data_url)
        #response = urllib.urlopen('requests_unit_test.xml')
    except IOError:
        print 'IOError'
        append_log('err_log.txt', 'IOError');
        return None

    return response

def parse_and_store_data(response, start_date):
    """Parsing XML data from San Francisco's Open311 endpoint and storing it in a postgres database"""

    import xml.dom

    reqs = []

    # Lookup table: use a set since we don't need to associate the the attributes with values
    # May want to add 'updated' flag
    relevant_attrs = {'service_request_id', 'status', 'service_name', 'service_code', 'description',
                        'requested_datetime', 'updated_datetime','expected_datetime', 'address', 'zipcode', 'lat', 'long'}

    try:
        print 'response'
        dom = minidom.parse(response)
    except xml.parsers.expat.ExpatError:
        print 'Expat error'
        append_log('err_log.txt', 'ExpatError. Start date: ' + days[start.weekday()] + ', ' + start.strftime('%Y-%m-%d'))
        return

    for node in dom.getElementsByTagName('request'):
        req_obj = {}

        for attr in node.childNodes:
            if attr.nodeType != xml.dom.Node.ELEMENT_NODE:
                continue
            if attr.childNodes:
                if attr.tagName in relevant_attrs:
                    # http://wiki.postgresql.org/wiki/Introduction_to_VACUUM,_ANALYZE,_EXPLAIN,_and_COUNT // Don't insert null value?
                    req_obj[attr.tagName] = attr.childNodes[0].data or None # will this work?
        # Check if you have a complete set of data for the request
        for relevant_attr in relevant_attrs:
            if relevant_attr not in req_obj:
                req_obj[relevant_attr] = None # To insert null values either omit the field from the insert statement or use None
        
        # Rename the long attribute
        req_obj['lon'] = req_obj['long']
        del req_obj['long']
    
        #print req_obj['zipcode']
        
        if req_obj['zipcode']:
            if not is_number(req_obj['zipcode']):
                req_obj['zipcode'] = None

        if float(req_obj['lat']) > 35 and float(req_obj['lon']) < -121:
            reqs.append(req_obj)

    append_log('log.txt', str(len(reqs)) + ' requests, start date: ' + start.isoformat() + ', ' + str(datetime.datetime.utcnow()) + '\n')
    
    #print 'reqs', reqs
    
    update_database(reqs)

def update_database(reqs):
    """Inserting and updating Open311 data in our postgres database."""

    #create schema
    """
    {u'status': u'Open', 'description': None, u'service_code': u'021', u'service_name': u'Pavement_Defect', 
    u'service_request_id': u'1132477', 'updated_datetime': None, u'zipcode': u'94134', 'lon': u'-122.39712', 
    u'requested_datetime': u'2012-05-26 23:36:16.303', u'address': u'510  BLANKEN AVE SAN FRANCISCO , CA ', 
    u'lat': u'37.710915', 'expected_datetime': None}
    """

    # to update
    # https://open311.sfgov.org/dev/Open311/v2/requests.xml?service_request_id=1132477,1132476&jurisdiction_id=sfgov.org
    # this gives all the requests on a particular date:
    # https://open311.sfgov.org/Open311/v2/requests.xml?end_date=2012-05-27T00%3A00%3A00Z&start_date=2012-05-26T00%3A00%3A00Z&jurisdiction_id=sfgov.org
    # longest request to be fulfilled?
    
    conn = psycopg2.connect(host=config['DATABASE']['host'], password=config['DATABASE']['password'], dbname=config['DATABASE']['db_name'], user=config['DATABASE']['user'])
    cur = conn.cursor()

    count = 0
    
    try:
        #http://wiki.postgresql.org/wiki/Psycopg2_Tutorial (execute many)
        for req in reqs:
            #print req
            #print '\n'
            print count
            count = count + 1
            
            # Check to see if we have the request and it needs to be updated
            cur.execute("SELECT service_request_id FROM sf_requests WHERE service_request_id = %s", (req['service_request_id'],))
            res = cur.fetchone()

            if res: # Make this test more explicit
                print 'Updating'
                # will update without a where always add a new result?
                # don't update expected datetime if it is now null
                cur.execute("""UPDATE sf_requests SET service_request_id=%(service_request_id)s, service_name=%(service_name)s, service_code=%(service_code)s,
                             description=%(description)s, status=%(status)s, lat=%(lat)s, lon=%(lon)s, requested_datetime=%(requested_datetime)s,
                             expected_datetime=%(expected_datetime)s, updated_datetime=%(updated_datetime)s, address=%(address)s, zipcode=%(zipcode)s
                             WHERE service_request_id=%(service_request_id)s""", req)
            else:
                print 'inserting'
                #print req
                
                print 'Getting the neighborhood'
                
                cur.execute("""SELECT neighborho from pn_geoms WHERE ST_INTERSECTS(geom, SET_SRID(ST_MakePoint((%s),(%s)), 4326))""", (req['lon'], req['lat']))
                
                neighborhood = cur.fetchone()
                
                print 'neighborhood', neighborhood
                
                if neighborhood:
                    req['neighborhood'] = neighborhood[0]
                else:
                    req['neighborhood'] = None
                    
                
                service_list = {"1":"Garbage","2":"Sidewalk or Street","3":"Sidewalk or Street",
                    "4":"Garbage","5":"Sidewalk or Street","6":"Trees","7":"Defacement / Graffiti",
                    "8":"Defacement / Graffiti","9":"Defacement / Graffiti","10":"Defacement / Graffiti",
                    "11":"Defacement / Graffiti","12":"Defacement / Graffiti","13":"Defacement / Graffiti",
                    "15":"Garbage","16":"Trees","17":"Sidewalk or Street","18":"Sidewalk or Street",
                    "19":"Garbage","20":"Trees","21":"Sidewalk or Street","22":"Sewage","23":"Sewage",
                    "24":"Sidewalk or Street","25":"Sidewalk or Street","26":"Sidewalk or Street","27":"Defacement/Graffiti",
                    "29":"Defacement / Graffiti","30":"Sewage","31":"Sewage","32":"Sewage","33":"Vehicles","44":"Garbage",
                    "47":"Water","48":"Garbage","49":"Defacement / Graffiti","68":"Defacement / Graffiti","172":"Garbage","174":"Garbage",
                    "176":"Trees","233":"Sewage","235":"Water","307":"Sewage","313":"Sewage","314":"Sidewalk or Street",
                    "331":"Garbage","332":"Trees","333":"Trees","336":"Trees","337":"Trees","365":"Trees","375":"Sidewalk or Street",
                    "376":"Sidewalk or Street","377":"Sidewalk or Street","378":"Sidewalk or Street","379":"Sidewalk or Street"}
                                
                stripped_service_code = req['service_code'].lstrip('0')
                
                if stripped_service_code in service_list:
                    category = service_list[stripped_service_code]
                    req['category'] = category
                else:
                    req['category'] = None
                    
                print req['category']
                
                cur.execute(
                    """INSERT INTO sf_requests (service_request_id, service_name, service_code, description, status, lat, lon, requested_datetime,
                        expected_datetime, updated_datetime, address, zipcode, neighborhood, category) VALUES (%(service_request_id)s, %(service_name)s, %(service_code)s,
                        %(description)s, %(status)s, %(lat)s, %(lon)s, %(requested_datetime)s, %(expected_datetime)s, %(updated_datetime)s,
                        %(address)s, %(zipcode)s, %(neighborhood)s, %(category)s);""", req)
                        
    except psycopg2.IntegrityError:
        conn.rollback()
    except Exception as e:
        print e

    conn.commit()
    # http://initd.org/psycopg/docs/connection.html#connection.autocommit
    cur.close()
    conn.close()

if __name__ == '__main__':
    from optparse import OptionParser

    parser = OptionParser()

    default_end_date = datetime.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=1)

    """
        Edit db_config_sample.json to include the specifics about your postgres instance.
        Rename the file to db_config.json.
    """
    
    defaults = {'config': 'db_config.json', 'end_date': datetime.datetime.strftime(default_end_date,'%Y-%m-%d'), 'num_of_days': 1}

    parser.set_defaults(**defaults)
    
    parser.add_option('-c', '--config', dest='config', help='Provide your configuration file.')
    parser.add_option('-e', '--end_date', dest='end_date', help='Provide the end date in the form YYYY-MM-DD')
    parser.add_option('-n', '--num_of_days', dest='num_of_days', type='int', help='Provide the number of days.')
    options, args = parser.parse_args()

    if (options.config and options.end_date and options.num_of_days):
        print options.end_date
        config = load_config(options.config) #global?
        end_date = datetime.datetime.strptime(options.end_date, '%Y-%m-%d')        
        num_of_days = options.num_of_days

        start, end = compute_time_range(end_date, 1) # Just handling one day at a time

        for day in xrange(num_of_days):
            print start.isoformat() + ' ' + end.isoformat()
            
            response = get_requests(start, end)
            
            if response:
                parse_and_store_data(response, start) # Handle Expat error, just get data for a day
            else:
                append_log('err_log.txt', 'Could not get a response for the following range (start - end): ' + start.isoformat() + ' - ' + end.isoformat())
                
                continue

            start -= ONE_DAY
            end -= ONE_DAY
    else:
        # not necessary with defaults
        print "You need to run this program like so: python update_postgres.py --config db_config.json --end_date YYYY-MM-DD --num_of_days 1."
