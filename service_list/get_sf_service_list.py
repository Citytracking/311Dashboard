import urllib
from xml.dom import minidom
import xml.parsers.expat
import json

services_url = 'https://open311.sfgov.org/v2/services.xml?jurisdiction_id=sfgov.org'

response = urllib.urlopen(services_url)

try:
	dom = minidom.parse(response)
except xml.parsers.expat.ExpatError, err:
	print 'ExpatError'
	sys.exit(1)

groups = {}

for node in dom.getElementsByTagName('service'):
	service_data = {}
	for attr in node.childNodes:
		if attr.childNodes:
			if attr.tagName == 'service_name' or attr.tagName == 'service_code' or attr.tagName == 'description':
				service_data[attr.tagName] = attr.childNodes[0].data
			elif attr.tagName == 'group':
				group_name = attr.childNodes[0].data
				if group_name not in groups:
					groups[group_name] = []
	groups[group_name].append(service_data)

with open('service_list.json', 'wt') as f:
    f.write(json.dumps(groups, sort_keys=True, indent=2))