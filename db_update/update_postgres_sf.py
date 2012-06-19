import sys
import datetime
import urllib
# from xml import dom, parsers
from xml.dom import minidom
import xml.parsers.expat
import psycopg2
import json

ONE_DAY = datetime.timedelta(1)

def load_config(filename):
    with open(filename, 'rt') as f:
        config = json.load(f)
        
    return config

def append_log(file_name, message):
    with open(file_name, 'a') as log_file:
        log_file.write(message)
    
def compute_time_range(end_date=None, num_of_days=1):
    """Computing the the start and end date for our Open311 query"""

    days_delta = datetime.timedelta(days=num_of_days)

    if end_date is None:
        end_date = datetime.datetime.utcnow() - datetime.timedelta(days=1)

    end = end_date.replace(hour=0, minute=0, second=0, microsecond=0) + ONE_DAY # Making sure we get everything

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
        dom = minidom.parse(response)
    except xml.parsers.expat.ExpatError:
        append_log('err_log.txt', 'ExpatError');
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

        reqs.append(req_obj)

    append_log('log.txt', str(len(reqs)) + ' requests, start date: ' + start_date + ', ' + str(datetime.datetime.utcnow()) + '\n')
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
        
    conn = psycopg2.connect(host=config['DATABASE']['host'], password=config['DATABASE']['password'], dbname=config['DATABASE']['db_name'], user=config['DATABASE']['user'])
    cur = conn.cursor()

    try:
        #http://wiki.postgresql.org/wiki/Psycopg2_Tutorial (execute many)
        for req in reqs:
            # Check to see if we have the request and it needs to be updated
            cur.execute("""SELECT service_request_id FROM sf_requests WHERE service_request_id = %s""", (req['service_request_id'],))
            res = cur.fetchone()

            if res: # Make this test more explicit
                print 'Updating'
                cur.execute("""UPDATE sf_requests SET service_request_id=%(service_request_id)s, service_name=%(service_name)s, service_code=%(service_code)s,
                             description=%(description)s, status=%(status)s, lat=%(lat)s, lon=%(lon)s, requested_datetime=%(requested_datetime)s,
                             expected_datetime=%(expected_datetime)s, updated_datetime=%(updated_datetime)s, address=%(address)s, zipcode=%(zipcode)s
                             WHERE service_request_id=%(service_request_id)s""", req)
            else:
                cur.execute(
                    """INSERT INTO sf_requests (service_request_id, service_name, service_code, description, status, lat, lon, requested_datetime,
                        expected_datetime, updated_datetime, address, zipcode) VALUES (%(service_request_id)s, %(service_name)s, %(service_code)s,
                        %(description)s, %(status)s, %(lat)s, %(lon)s, %(requested_datetime)s, %(expected_datetime)s, %(updated_datetime)s,
                        %(address)s, %(zipcode)s);""", req)
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
            response = get_requests(start, end)
            
            if response:
                parse_and_store_data(response, start.isoformat()) # Handle Expat error, just get data for a day
            else:
                append_log('err_log.txt', 'Could not get a response for the following range (start - end): ' + start + ' - ' + end)
                
                continue

            start -= ONE_DAY
            end -= ONE_DAY
    else:
        # not necessary with defaults
        print "You need to run this program like so: python update_postgres.py --config db_config.json --end_date YYYY-MM-DD --num_of_days 1."