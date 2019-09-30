import random
import sys
from threading import Thread
import time
import subprocess
import argparse
import mysql.connector

class Dumper(Thread):
    def __init__(self,thread,db,table,user,password,host,folder,compress):
        Thread.__init__(self)
        self.subcmd=""
        self.bashCommand = "mysqldump --quick -h "+str(host)+" -u"+str(user)+" -p"+str(password)+" "+str(db)+" "+str(table)
        if thread <= 1 and compress !=0 :
            self.subcmd="pigz -9 -p 1"
            self.myoutput = open(str(folder)+"/dmp_"+str(db)+"/"+str(table)+".sql.gz",'w+')
        elif thread > 1 and compress !=0:
            self.subcmd="pigz -9 -p "+str(thread)+""
            self.myoutput = open(str(folder)+"/dmp_"+str(db)+"/"+str(table)+".sql.gz",'w+')
        else:
            self.myoutput = open(str(folder)+"/dmp_"+str(db)+"/"+str(table)+".sql",'w+')

    def run(self):
        if self.subcmd !="":
            process_dump = subprocess.Popen(self.bashCommand.split(),stderr=subprocess.DEVNULL, stdout=subprocess.PIPE,shell=False)
            process_compress = subprocess.run(self.subcmd.split(),stderr=subprocess.DEVNULL, stdin=process_dump.stdout, stdout=self.myoutput,shell=False)

        else:
            process_dump = subprocess.run(self.bashCommand.split(), stdout=self.myoutput)
        #output, error = process.communicate()
        #print(self.bashCommand)



parser = argparse.ArgumentParser(description='This is MysqlDumper! Home made, Multithreaded mysqldump ')
parser.add_argument('-H', action='store', dest='host', default="127.0.0.1",help='Host to dump')
parser.add_argument('-u', action='store', dest='user',help='User to use for dump your mysql Server')
parser.add_argument('-F', action='store', dest='folder',help='Path of folder backup')
parser.add_argument('-c', action='store', dest='compress',help='Compress in gzip format ?',type=int, default=0)
parser.add_argument('-p', action='store', dest='password',help='Password of the specified user')
parser.add_argument('-P', action='store', dest='parallel',help='Number of threads',type=int,default=1)
parser.add_argument('-D', action='store', dest='databases',help='Databases you want to dump, if multiples DB please use delimiter \',\'',default=[])
parser.add_argument('--version', action='version', version='%(prog)s 1.0')
results = parser.parse_args()



mydb = mysql.connector.connect(host=results.host,user=results.user,passwd=results.password)
nb_threads=results.parallel
compress=results.compress
path=results.folder
db_list=results.databases.split(",")
print(db_list)
for db in db_list:
    myresult= []
    mycursor = mydb.cursor()
    print("We are currently working on :"+str(db))
    Command = "mkdir "+str(results.folder)+"/dmp_"+str(db)
    process = subprocess.Popen(Command.split(), stdout=subprocess.PIPE)
    mycursor.execute("use "+str(db))
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
        to_del=""
        if rest == 0:
            data=[None]*definitive_threads
            for i in range(definitive_threads):
                data[i]=Dumper(nb_threads,db,table_list[i],results.user,results.password,results.host,results.folder,compress)
                data[i].start()
            for i in range(definitive_threads):
                data[i].join()
                to_del=int(definitive_threads)
            del table_list[0:to_del]
        else:
            data=[None]*(int(rest)+1)
            for i in range(rest):
                data[i]=Dumper(nb_threads,db,table_list[i],results.user,results.password,results.host,results.folder,compress)
                data[i].start()
            for i in range(rest):
                data[i].join()
                to_del=int(rest)              
            rest=(table_list_len-to_del)%definitive_threads
            del table_list[0:to_del]

