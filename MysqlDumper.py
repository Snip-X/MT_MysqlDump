import random
import sys
from threading import Thread
import time
import subprocess
import argparse
import mysql.connector

class Dumper(Thread):
    def __init__(self,db,table,user,password,host,folder):
        Thread.__init__(self)
        self.bashCommand = "mysqldump --quick -h "+str(host)+" -u"+str(user)+" -p"+str(password)+" "+str(db)+" "+str(table)
        self.myoutput = open(str(folder)+"/dmp_"+str(db)+"/"+str(table)+".sql",'w+')
        #" > "+

    def run(self):
        
        process = subprocess.run(self.bashCommand.split(), stdout=self.myoutput)
        #output, error = process.communicate()
        #print(self.bashCommand)



parser = argparse.ArgumentParser(description='This is MysqlDumper! Home made, Multithreaded mysqldump ')
parser.add_argument('-H', action='store', dest='host', default="127.0.0.1",help='Host to dump')
parser.add_argument('-u', action='store', dest='user',help='User to use for dump your mysql Server')
parser.add_argument('-F', action='store', dest='folder',help='Path of folder backup')
parser.add_argument('-p', action='store', dest='password',help='Password of the specified user')
parser.add_argument('-P', action='store', dest='parallel',help='Number of threads',type=int,default=1)
parser.add_argument('-D', action='store', dest='databases',help='Databases you want to dump, if multiples DB please use delimiter \',\'',default=[])
parser.add_argument('--version', action='version', version='%(prog)s 1.0')
results = parser.parse_args()



mydb = mysql.connector.connect(host=results.host,user=results.user,passwd=results.password)
mycursor = mydb.cursor()
nb_threads=results.parallel
path=results.folder
db_list=results.databases.split(", ")
for db in db_list:
    Command = "mkdir "+str(results.folder)+"/dmp_"+str(db)
    process = subprocess.Popen(Command.split(), stdout=subprocess.PIPE)
    mycursor.execute("use "+str(results.databases))
    mycursor.execute("show tables")
    myresult = mycursor.fetchall()
    table_list= [x[0] for x in myresult]
    table_list_len= len(table_list)
    ##Check si les threads ne sont pas abuse
    if table_list_len<nb_threads:
        definitive_threads=table_list_len
    else:
        definitive_threads=nb_threads
    rest=table_list_len%definitive_threads
    while len(table_list)>0:
        if rest == 0:
            data=[None]*definitive_threads
            for i in range(definitive_threads):
                data[i]=Dumper(db,table_list[i],results.user,results.password,results.host,results.folder)
                data[i].start()
                table_list.remove(table_list[i])
            for i in range(definitive_threads):
                data[i].join()
        else:
            data=[None]*rest
            for i in range(rest):
                data[i]=Dumper(db,table_list[i],results.user,results.password,results.host,results.folder).start()
                table_list.remove(table_list[i])
            rest=(table_list_len-i)%definitive_threads

