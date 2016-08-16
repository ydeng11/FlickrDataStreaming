# -*- coding:utf-8 -*-
import flickr_api
from flickr_api.api import flickr
import json
import MySQLdb
from datetime import datetime
# from pandas.io.json import json_normalize

my_api_key = 'a998f2b9e2d10f658ea87c00cb2b92e1'
my_secret = '0960486f0335c314'
flickr_api.set_keys(api_key = my_api_key, api_secret = my_secret)
auth = flickr_api.auth.AuthHandler() #creates the AuthHandler object
# auth.set_verifier = '72157671561687371-cbb80beb464db307'
# flickr_api.set_auth_handler(auth)
# perms = "read" # set the required permissions
# url = auth.get_authorization_url(perms)
# print url
# print flickr.reflection.getMethodInfo(method_name = "flickr.photos.search")

class flickrDataCollection:
	def __int__(self):
		self.bbox = "-82.64, 27.64, -82.05, 28.17" #The leftbottom and upright cornor of Tampa
		self.accuracy = 16 #street level
		self.content_type = 1 #photos only
		self.per_page = 10 #the number of data in each page
		self.page = 500 # the maximum is 500
		self.start_date = "2005-08-01" #start date
		self.end_date = "2016-08-01" #end-date
		self.table = "flickr_data" 


	def getPhotos(self, parameter):
		# print flickr.photos.geo.getLocation()
		geoPhotos = flickr.photos.search(bbox=parameter["bbox"], accuracy=parameter["accuracy"],content_type=parameter["content_type"],
									per_page = parameter["per_page"], page =parameter["page"], min_taken_date = parameter["bbox"],
									max_taken_date = parameter["max_taken_date"], format='json')
		# test = xmltodict.parse(geoPhotos)
		# print test.values()
		# print geoPhotos
		id_list = []
		geoPhotos = geoPhotos.lstrip("jsonFlickrApi(")
		geoPhotos = geoPhotos.rstrip(")")
		# print geoPhotos
		parsed_text = json.loads(geoPhotos)
		# print type(parsed_text['photos']['photo'])
		for photo in parsed_text['photos']['photo']:
			id_list.append(photo['id'])
			print "The photo %s is collected." %photo['id']
		return id_list

#get the information and location of photos
	def getInfo(self,id):
		tag = []
		text = flickr.photos.getInfo(photo_id='5248964236', format='json')
		text = text.lstrip('jsonFlickrApi(')
		text = text.rstrip(')')
		parsed_text = json.loads(text)
		text_tag = parsed_data['photo']['tags']['tag']
		for line in text_tag:
			tag.appendline(['_content'])
		longitude = parsed_data['photo']['location']['longitude']
		latitude = parsed_data['photo']['location']['latitude']
		time = parsed_data['photo']['dates']['taken']
		url_loc = parsed_data['photo']['urls']['url']
		url = url_loc[0]['_content']
		location = parsed_data['photo']['location']['locality']['_content']
		title = parsed_data['photo']['title']['_content']
		photo_id = parsed_data['photo']['id']
		info = {
				"photo_id":photo_id,
				"title":title,
				"tag":tag,
				"taken_time":time,
				"coordinates": (latitude, longitude),
				'location':location,
				'url':url
		}
		print "The information of photo %s is collected." %photo_id
		return info

	def savetomysql(self, table, data):
		try:
			db = MySQLdb.connect(host = 'localhost',
								 user = 'root',
								 password = '891105',
								 db = 'mysql'
				)
			cur = db.cursor()
		except MySQLdb.error, e:
			print "The error found in connnecting database%d: %s" % (e.args[0], e.args[1])
		try:
			self.db.set_character_set('utf8')
			cols = ', '.join(str(v) for v in data.keys())
			values = '"'+'","'.join(str(v) for v in data.values())+'"'
			sql = "INSERT INTO %s (%s) VALUES (%s)" % (table, cols, values)
			cur.excute()
			try:
				result = cur.execute(sql)
				db.commit()
				#Check the result of command execution
				if result:
					print "This data is input into database."
				else:
					return 0
			except MySQLdb.Error,e:
				 #rollback if error
				self.db.rollback()
				 #duplicate primary key
				if "key 'PRIMARY'" in e.args[1]:
					print self.getCurrentTime(),"Data Existed"
				else:
					print self.getCurrentTime(),"Insertion faied, reason is %d: %s" % (e.args[0], e.args[1])
		except MySQLdb.Error,e:
			print self.getCurrentTime(),"Error found in database, reason is %d: %s" % (e.args[0], e.args[1])


	def main(self):
		difftime = datetime.strptime(end_date, format_time) - datetime.strptime(start_date, format_time)
		intervals = difftime/365 #the difference by years and minus 1 for loop
		year = timedelta(days=365)
		for i in range(0, interval):
			min_taken_date = str(datetime.strptime(start_date, format_time) + year*i)
			max_taken_date = str(datetime.strptime(start_date, format_time) + year*(i+1))
			print "Collection start time is " + min_taken_date + "Collection end time is " + max_taken_date
			parameter = {"bbox":self.bbox,
						 "accuracy":self.accuracy,
						 "content_type":self.content_type,
						 "per_page":self.per_page,
						 "page": self.page,
						 "min_taken_date":min_taken_date,
						 "max_taken_date":max_taken_date
						}
			photoIDs = getPhotos(parameter)
			try:
				for photoID in photoIDs: 
					photoInfo = getInfo(photoID)
					savetomysql(self.table, photoInfo)
			except:
				Exception
				print "Programmed stopped"

flickr_downloader = flickrDataCollection()
flickr_downloader.main()