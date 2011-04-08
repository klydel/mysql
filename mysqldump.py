#!/usr/bin/python
import MySQLdb
import base64
import os, sys
import datetime
import boto
import bz2
import logging
import datetime
import shutil

################################
mybucket = 'my-s3-mysql-backup'
subject = 'MySQL Backup'
programroot = '/opt/mysqldump'
today = datetime.date.today()
yesterday = today - datetime.timedelta(1)
hostn = os.uname()[1]'
#use base64 just to kind of hide stuff
p = base64.b64decode("MYBASE64ENCODEDPASSWORD")
#port to password mappings
#in this example we would have password = mysqlpassword, then have pre and post fixed with character and numbers.
#dbconf = { 3306 : 'f,6' }
#we would use the following if weve set up multiple mysql instances on the same host, replication slave for instance
dbconf = { 3301 : 'a,1', 3303 : 'c,3', 3304 : 'd,4', 3305 : 'e,5', 3306 : 'f,6' }
#the following are your amazon keys encoded in base64:
kaccess = ''
ksecret = '' 
log_filename = str(programroot + "/" + str(today) +".log")
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s', filename=log_filename, filemode='w')
start = datetime.datetime.now()
bodys = "MySQL Backup\n"
###############################


def mk_today_dir():
	path = programroot+ "/" +str(today)
	if os.path.isdir(path):
		pass
	else:
		try:
			os.mkdir(path)
		except IOError, error:
			logging.info(error)

def get_db_connection(port, passwd):
	try:
		db=MySQLdb.connect(host="127.0.0.1",user="root",passwd=passwd,port=port)
		c = db.cursor()
		return c
	except:
		logging.info('get_db_connection - Unable to Connect to DB on port : %s' +port)
	
def close_db_connection(db):
	db.close()

def show_databases(c):
	c.execute("""SHOW DATABASES""")
	return c

def show_tables(db):
	c.execute("USE "+db[0])
	c.execute("""SHOW TABLES""")
	return c

def s3_connection(kaccess, ksecret):
	from boto.s3.connection import S3Connection
	sconn = S3Connection(base64.b64decode(kaccess), base64.b64decode(ksecret))
	return sconn

def s3_close(sconn):
	try:
		sconn.close()
	except:
		pass

def create_bucket(sconn):
	try:
		bckt = sconn.create_bucket(mybucket)
		return bckt
	except:
		bckt = boto.s3.bucket.Bucket(sconn, mybucket)
		logging.info('create_bucket - Bucket Exists: %s' +mybucket)
		return bckt 
	

def upload_s3(sconn, bckt, mysqlfile):
	from boto.s3.key import Key
	try:
		bcktkey = Key(bckt)
	except:
		logging.info('upload_s3 - Unable to get Bucket')
	#bcktkey.key = str(today)+ "-" +hostn+ "-" +str(port) +"/" +mysqlfile.split('/')[::-1][0]
	bcktkey.key = str(today)+ "/" +mysqlfile.split('/')[::-1][0]
	bcktkey.set_contents_from_filename(str(mysqlfile))


def split_file(mysqlfile):
	try:	
		import glob
		split_command = "split -b 1024m %s %s.part-" % (mysqlfile, mysqlfile)
		split_status = os.popen(split_command).read()
		splitfilelist = glob.glob("%s.part-*" % (mysqlfile))
		return splitfilelist	
	except:
		logging.info('split_file - Unable to split file: ' +mysqlfile)
		
	
def compress_file(uncompressed_file):
	try:
		compress_me = "bzip2 %s" % (uncompressed_file)
		os.popen(compress_me)
	except:
		logging.info('compress_file - failed compressed %s' +uncompessed_file)
		pass

def send_alert(bodys):
	try:
		import smtplib
		fromaddr = ''
		toaddr = ''
		msg = ("From: %s\r\nTo: %s\r\nSubject: %s\r\n" % (fromaddr, toaddr, subject))
		msg = msg + bodys
		server = smtplib.SMTP('localhost')
		server.set_debuglevel(1)
		server.sendmail(fromaddr, toaddr, msg)
		server.quit()
	except:
		logging.info('send_alert - Failed to Send Alert ')

mk_today_dir()
for port, fix in dbconf.items():
	a, b = fix.split(',')
	c = get_db_connection(port, a+ p.strip() +b)
	for database in show_databases(c):
		backupstatus = 0
		path = programroot+ "/" +str(today)+ "/" + str(port) + "/" +database[0]+ "/"
		try:
			os.makedirs(path)
			pass
		except IOError, error:
			logging.info(error)
			backupstatus = backupstatus + 1
		for table in show_tables(database):
			mysqlfile = str(path) +str(today)+ "-" +database[0]+ "-" +table[0] +".sql"
			sql = "mysqldump -u root -h 127.0.0.1 -p%s -P%s %s %s > %s" % ((a+ p.strip() +b), port, database[0], table[0], str(mysqlfile))
			dumpstatus = os.popen(sql).read()
			if dumpstatus != None:
				try:
					compress_file(str(mysqlfile))
				except:
					logging.info('Compression Failed for ' +str(mysqlfile))
					backupstatus = backupstatus + 1
					pass
				sconn = s3_connection(kaccess, ksecret)
				bckt = create_bucket(sconn)
				try:
					upload_s3(sconn, bckt, str(mysqlfile) + ".bz2")
				except:
					try:
					
						split_bz2_files = split_file(str(mysqlfile) + ".bz2")
						for i in split_bz2_files:
							upload_status = upload_s3(sconn, bckt, str(i))
							if upload_status != None:
								backupstatus = backupstatus + 1
						logging.info('Finally - Uploaded all split files ' +str(split_bz2_files))
					except:
						logging.info('upload_s3 - Unable to Upload ' +str(mysqlfile))
						backupstatus = backupstatus + 1
			else:
				logging.info('table backup error: ' +database[0] + table[0])
				backupstatus + 1
				os.remove(str(mysqlfile))
		bodys = bodys + ("Datbase %s had: %s errors during backup\r\n\r\n" % (database[0], backupstatus))
		s3_close(sconn)
close_db_connection(c)
stop = datetime.datetime.now()
elapsed = stop - start
bodys = bodys + ("Time of Total Run: %s\n " % (elapsed))
try:
	shutil.rmtree('/opt/mysqldump/' +str(yesterday))
except:
	pass
logging.shutdown()
send_alert(bodys)


