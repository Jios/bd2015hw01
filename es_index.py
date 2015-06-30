#!/usr/bin/python

from elasticsearch import Elasticsearch
from geoip import geolite2
from glob  import glob
from dateutil.parser import parse
import re, email, pycountry, io, base64
from email.header import decode_header


##############################################################


""" 
######################
#  global variables  #
######################
"""

DEBUG 			= False #True

es_index_name   = "test" #"bigdata_hw1"
es_doc_type 	= "spam_email"

if DEBUG:
	path = "datasets/spam_emails/2014/01/1388597503.*.lorien"
else:
	path = "datasets/spam_emails/2014/01/*"

# ReGex
# res = re.compile(r"^\w*: ")
reIPmeta	= re.compile(r"^from (?P<from>.*) \(.*\[(?P<ip>\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b)\]")

reIP 		= re.compile(r"^Received: from (?P<from>.*) \(.*\[(?P<ip>\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b)\]")
reFrom		= re.compile(r"^From:.*?\<(?P<sender_email>.*)\>")
reTo		= re.compile(r"^To:.*?\<(?P<receiver_emaill>.*)\>")
reSubject 	= re.compile(r"^(?P<key>Subject: )(?P<subject>.*)")
reCtype 	= re.compile(r"^Content-Type: (?P<ctype>.*); charset=\"(?P<charset>.*)\"")
reClen      = re.compile(r"^Content-Length: (?P<clen>.*)")

# elasticsearch: by default we connect to localhost:9200
es 			= Elasticsearch()


##############################################################


""" 
###############
#  functions  #
###############
"""

def delete_es_index():
	# delete index from elasticsearch
	if not DEBUG:
		es.indices.delete(index = es_index_name, ignore = [400, 404])

def es_index(docID, es_dict):
	#  send data to elasticsearch
	if not DEBUG:
		es.index(index = es_index_name, doc_type = es_doc_type, id = docID, body = es_dict)

def es_search(docID):
	# search by doc ID
	result = es.get(index = es_index_name, doc_type = es_doc_type, id = docID)['_source']
	return result

##############################################################

def unicodish(s):
	return s.decode('latin-1', errors = 'replace')

def base64Decode(text):
	return base64.b64decode(text)

##############################################################

def parse_spam(filename, lines):
	# parse spam email

	# elasticsearch data
	es_dict = {}
	charset = None

	for line in lines:
		# match
		ip_match = reIP.match(line)
		sender_email_match = reFrom.match(line)
		receiver_emaill_match = reTo.match(line)
		subject_match = reSubject.match(line)
		contentType_match = reCtype.match(line)
		clen_match = reClen.match(line)

		# ip match
		if ip_match:
			# sender_addr = ip_match.group("from")
			sender_ip = ip_match.group("ip")

			# geolocation based on IP address
			geolocation = geolite2.lookup(sender_ip)
			if geolocation is not None:
				print filename + " - sender IP: " + ip_match.group("ip")

				es_dict.update(geolocation.get_info_dict()) #to_dict()) 
				es_dict["sender_IP"] = geolocation.ip
				# es_dict["sender_address"] = sender_addr
		elif sender_email_match:
			es_dict["sender_email"] = sender_email_match.group("sender_email")
		elif receiver_emaill_match:
			es_dict["receiver_emaill"] = receiver_emaill_match.group("receiver_emaill")
		# subject match
		elif subject_match:
			subject = subject_match.group("subject")
			# if type(subject) == unicode:
			#     data = bytearray(subject, "utf-8")
			#     subject = bytes(data)
			es_dict["subject_length"] = len(subject)
			# es_dict["subject"] = unicode(subject)
		# content-type and charset matches
		elif contentType_match:
			es_dict["charset"] 		  = contentType_match.group("charset")
			es_dict["content_type"]   = contentType_match.group("ctype")
		elif clen_match:
			es_dict["content_length"] = clen_match.group("clen")

	return es_dict

def parse_metadata(message, es_dict):
	# parase metadata of spam email file

	for k, v in message.items():
		line = k + ": " + v

		if k == "Received":
			ip_match = reIP.match(line)
			if ip_match:
				sender_ip = ip_match.group("ip")
				getGeoDict(sender_ip, es_dict)
		elif k == "From":
			from_match = reFrom.match(line)
			if from_match:
				sender_email = from_match.group("sender_email")
				v = sender_email
		elif k == "To":
			to_match = reTo.match(line)
			if to_match:
				v = to_match.groups()[0]
				
		if k == "Subject" and v[0:2] == "=?":
			text_list = filter(lambda m: len(m)>0, v.split("?="))
			v = ""
			for text in text_list:
				if text[0:2] == "=?":
					v = v + getheader(text+"?=")
				else:
					v = v + v
			# v = getheader(v)
		else:
			v = unicodish(v)

		es_dict[unicodish(k).lower()] = v #unicodish(v)

def parse_content(message, es_dict):
	# parse spam content

	content = io.StringIO()
	if message.is_multipart():
	    for part in message.get_payload():
	        if part.get_content_type() == 'text/plain':
	            content.write(unicodish(part.get_payload()))
	else:
	    content.write(unicodish(message.get_payload()))

	es_dict["content"] = content.getvalue()

##############################################################

def getheader(header_text, default="ascii"):
	print "====="
	# print header_text
	# header_text = re.match(r"=\?.*\?=", header_text).group()
	print header_text
	headers = decode_header(header_text)
	print headers

	try:
		for text, charset in headers:
			print unicode(text, charset or default)
		header_sections = [unicode(text, charset or default) for text, charset in headers]
	except:
		print "LookupError"
		return header_text
	else:
		return u"".join(header_sections)

    # header_sections = [unicode(text, charset or default) for text, charset in headers]
    # return u"".join(header_sections)

def getGeoDict(sender_ip, es_dict):
    # get geolocation dictionary from IP

    geo_dict = {}

    # geolocation based on IP address
    geolocation = geolite2.lookup(sender_ip)
    if geolocation is not None:
      info_dict = geolocation.get_info_dict() #to_dict()) 

      print "sender IP - " + sender_ip
      if info_dict.has_key("country"):
        cntr_dict = info_dict["country"]
        cntr_alpha2 = cntr_dict["iso_code"]
        cntr_alpha3 = pycountry.countries.get(alpha2 = cntr_alpha2).alpha3
        cntr_name   = cntr_dict["names"]["en"]

        geo_dict["geoip"] = {}

        geo_dict["geoip"] = {"ip": geolocation.ip, \
                   "country_code2": cntr_alpha2,\
                   "country_code3": cntr_alpha3,\
                   "country_name": cntr_name}
                   
        if info_dict.has_key("city") and info_dict["city"].has_key("geoname_id"):
          geo_dict["geoip"]["city_geoname_id"] = info_dict["city"]["geoname_id"]
        if info_dict.has_key("continent") and info_dict["continent"].has_key("geoname_id"):
          geo_dict["geoip"]["continent_geoname_id"] = info_dict["continent"]["geoname_id"]
        if info_dict.has_key("country") and info_dict["country"].has_key("geoname_id"):
          geo_dict["geoip"]["country_geoname_id"] = info_dict["country"]["geoname_id"]

    	if info_dict.has_key("location"):
    			lat = info_dict["location"]["latitude"]
    			lon = info_dict["location"]["longitude"]
    			geo_dict["geoip"].update({"ip": geolocation.ip, "continent_code": info_dict["continent"]["code"], "latitude": lat, "longitude": lon, "location": [lon, lat], "coordinates": [lon, lat]})
    			geo_dict["pin"] = {"location": [lon, lat]}
    			es_dict.update(geo_dict)
    return geo_dict

##############################################################

def main():
	# main function
	
	delete_es_index()

	docID = ""

	# loop through files
	for filename in glob(path):
		print filename
		docID = re.split(r".*data\w*\/", filename)[-1]

		# open and read a file
		with open(filename, 'r') as fp:

			es_dict = {}

			message = email.message_from_file(fp)

			parse_metadata(message, es_dict)
			parse_content( message, es_dict)
			
			if es_dict.has_key("date"):
				timestamp_str = es_dict.pop("date")
				es_dict["timestamp"] = parse(timestamp_str.replace(".", ":"))
			if es_dict.has_key("content-transfer-encoding"):
				if es_dict["content-transfer-encoding"] == "base64":
					es_dict["content"] = unicodish(base64Decode(es_dict["content"]))

			es_index(docID, es_dict)


##############################################################


if __name__ == '__main__':
	main()
