#! /usr/bin/env python3
# ~*~ utf-8 ~*~

# Readme :
# influx user create -n arias -p arias -o cyberneurones-org 

import mailbox
import bs4
import glob
import os
import time
import codecs
import sys
from influxdb import InfluxDBClient
import re
from datetime import datetime
from email.utils import parsedate_to_datetime
import logging
import psycopg2

logger = logging.Logger('catch_all')

#########################

global nb_folder
nb_folder = 0;
global nb_email
nb_email = 0;
global nb_error
nb_error = 0;
global id_email
id_email = 0;

global client
client = InfluxDBClient(host='127.0.0.1', port=8086, username='arias', password='arias')
client.drop_database('thunderbird')
client.create_database('thunderbird')
client.switch_database('thunderbird')

global client2
client2   = psycopg2.connect("dbname=thunderbird user=arias password='arias'")
cursor                = client2.cursor()
global name_Table
name_Table            = "thunderbird"
#sqlCreateTable = "create table "+name_Table+" (id bigint UNIQUE, mail varchar(128), name varchar(128), domain varchar (128), date timestamp);"
sqlCreateTable = "delete from "+name_Table;
cursor.execute(sqlCreateTable)
client2.commit()

#########################

def get_html_text(html):
    try:
        return bs4.BeautifulSoup(html, 'lxml').body.get_text(' ', strip=True)
    except AttributeError: # message contents empty
        return None

class GmailMboxMessage():
    def __init__(self, email_data):
        if not isinstance(email_data, mailbox.mboxMessage):
            raise TypeError('Variable must be type mailbox.mboxMessage')
        self.email_data = email_data

    def parse_email(self):
        global client
        global client2
        global id_email
        global name_Table
        global cursor
        email_date = self.email_data['Date']
        email_from = self.email_data['From']
        email_to = self.email_data['To']
        email_subject = self.email_data['Subject']
        if email_date is not None and email_from is not None:
            mail = re.search(r'[\w\.\-_]+@[\w\.\-_]+', email_from)
            if mail is not None:
                mailstr = mail.group(0)
            if mail is not None:    
                domain = re.search("@[\w\.\-_]+", email_from).group(0)
                domain = domain.replace('@', '')
                domain = domain.replace('>', '')
            if mail is not None:
                user = re.search("[\w\.i\-_]+@", email_from).group(0)
                user = user.replace('@', '')
                user = user.replace('<', '')
            local_time_str = datetime.fromtimestamp(parsedate_to_datetime(email_date).timestamp()).strftime('%Y-%m-%dT%H:%M:%S.%f%z')
            local_time_str2 = datetime.fromtimestamp(parsedate_to_datetime(email_date).timestamp()).strftime('%Y-%m-%d %H:%M:%S')
            timestamp = round(parsedate_to_datetime(email_date).timestamp() * 1000);
            if mail is not None:
                data = [{'measurement': 'thunderbirds', 'tags': { 'fullemail': 1, 'from': email_from, 'mail': mailstr, 'domain': domain, 'user': user}, 'id' : id_email, 'time': timestamp, 'date':local_time_str, 'fields': {"value": 1}}]
                sql = "INSERT INTO "+name_Table+" (mail, domain, name, id, date) VALUES ('" +mailstr+ "','" + domain+"','" +user+"','"+str(id_email)+"',TIMESTAMP '"+local_time_str2+"')";
                #print (sql)
                cursor.execute(sql);
                client2.commit();
            else :
               data = [{'measurement': 'thunderbirds', 'tags': { 'fullemail': 0, 'from': email_from }, 'id' : id_email, 'time': timestamp, 'date':local_time_str, 'fields': {"value": 1}}] 
            #print (data);
            client.write_points(data, time_precision='ms')
            id_email = id_email+1

def mbox_reader(stream):
    data = stream.read()
    text = data.decode(encoding="utf-8")
    return mailbox.mboxMessage(text)

######################### End of library, example of use below

Login = 'arias';
Folder = 'zy3zk9ms.default';

print("\nUsing glob.iglob()") 
for filename in glob.iglob('/home/'+Login+'/snap/thunderbird/common/.thunderbird/'+Folder+'/Mail/Local Folders/**/*', recursive=True): 
    print(filename);
    filename2, file_extension = os.path.splitext(filename);
    print(file_extension + " " + str(len(file_extension)));
    isFile = os.path.isfile(filename)
    if (file_extension != ".msf") and (file_extension != ".sbd") and isFile is True:
        mbox_obj = mailbox.mbox(filename);
        num_entries = len(mbox_obj)
        nb_folder = nb_folder + 1;
        try :
           for idx, email_obj in enumerate(mbox_obj):
               email_data = GmailMboxMessage(email_obj)
               email_data.parse_email()
               nb_email = nb_email + 1;
               print('Parsing email {0} of {1}'.format(idx, num_entries))
        except StopIteration:
           continue
        except Exception as e: 
           logger.error('Failed : '+ str(e))
           nb_error = nb_error+1;
           continue

print('The number of folder :'+str(nb_folder));    
print('The number of email :'+str(nb_email));
print('The number of error : '+str(nb_error));
print('The number in database : '+str(id_email));
