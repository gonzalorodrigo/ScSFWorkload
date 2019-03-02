from commonLib import tunnelLib
from commonLib.Logging import *
import datetime
import gc

import MySQLdb as mdb


class DB:
    hostName="localhost"
    dbName="nersc"
    userName="nersc"
    password="nersc"
    port="3306"
#    con=False    
    
    
    def __init__(self, hostName, dbName, userName, password, port="3306",
                 useTunnel=False, test_db_mysqld=None):
        self.con=False
        self.hostName=hostName
        self.dbName=dbName
        self.userName=userName
        self.password=password
        self.port=port
        self.useTunnel=useTunnel
        if (useTunnel):
            self.tunnel=tunnelLib.Tunnel()
        self._in_transaction=False
        self._cursor=None
        self._test_db_mysqld=test_db_mysqld
        
    def start_transaction(self):
        if not self.connect():
            raise Exception("Cannot connect at transaction start")
        self.con.autocommit(False)
        self.get_cursor()
        self._in_transaction=True
        
    
    def end_transaction(self):
        try:
            #self.con.rollback()
            self.con.commit()
        except mdb.Error as e:
            print("COMMIT FAILED!!", e)
        self._in_transaction=False
        self.disconnect()
        self._cursor.close()
        self._cursor=None
        
    def get_cursor(self):
        if not self._in_transaction or self._cursor==None:
            self._cursor=self.con.cursor()
        return self._cursor    
    
    def connect(self):
        if self._in_transaction:
            return True
                #print "DB", self.port
        if (self.useTunnel):
            self.tunnel.connect()
        try:
            if self._test_db_mysqld:
                self.con = mdb.connect(**self._test_db_mysqld.dsn()) 
            else:
                self.con = mdb.connect(self.hostName, self.userName, self.password, self.dbName, port=int(self.port))
    
            return True
        except mdb.Error as e:
            Log.log("Error %d: %s" % (e.args[0],e.args[1]))
            if (self.useTunnel):
              self.tunnel.disconnect()
            return False
        
    def disconnect(self):
        if (self.con==False or self._in_transaction):
            return
        self.con.close()
        self.con=False
        if (self.useTunnel):
            self.tunnel.disconnect()

    
    
    def date_to_mysql(self, my_date):
        return my_date.strftime('%Y-%m-%d %H:%M:%S')
    def date_from_mysql(self, my_date):
        return datetime.datetime.strptime(my_date, "%b %d %Y %H:%M")
    def doQuery(self, query):
                #print "QUERY:"+query
        rows = []
        if self.connect():
            try:
                cur=self.get_cursor()
                cur.execute(query)
                rows=cur.fetchall()    
            except mdb.Error as e:
                Log.log("Error %d: %s" % (e.args[0],e.args[1]))    
                rows=False
            self.disconnect()
        return rows
    
    # THe same but the result is a dictorionary list
    def doQueryDic(self, query):
        rows = []
                #print "QUERY:"+query

        if self.connect():
            try:
                cur=self.con.cursor(mdb.cursors.DictCursor)
                cur.execute(query)
                rows=cur.fetchall()    
    
            except mdb.Error as e:
                Log.log("Error %d: %s" % (e.args[0],e.args[1]))
                rows=False    
            self.disconnect()
        return rows
    
    def delete_rows(self, table, id_field, id_value, like_field=None,
                    like_value=None):
        query = "DELETE FROM `{0}` where `{1}`={2}".format(table, id_field,
                                                        id_value)
        if like_field is not None:
            query+=""" and `{0}` like "{1}" """.format(like_field,like_value)
        return self.doUpdate(query)
    
    def doUpdate(self, update, get_insert_id=False):
        ok=True
        insert_id=None
        if self.connect():
            try:
                cur=self.get_cursor()
            
                res=cur.execute(update)
                if get_insert_id:
                    insert_id = self.con.insert_id()
                if not self._in_transaction:
                    self.con.commit()

            
            except mdb.Error as e:
                Log.log("Error %d: %s" % (e.args[0],e.args[1]))    
                ok=False
            self.disconnect()
        return ok, insert_id
        
    def doUpdateMany(self, query, values):
        ok=True
        if self.connect():
            try:
                cur=self.get_cursor()
            
                print("Q;v", query, values)
                res=cur.executemany(query, values)
                if not self._in_transaction:
                    self.con.commit()
            
            except mdb.Error as e:
                Log.log("Error %d: %s" % (e.args[0],e.args[1]))    
                ok=False
            self.disconnect()
        return ok
    
    
    def doUpdateParams(self, update, params):
        ok=True
        if self.connect():
            try:
                cur=self.get_cursor()
            
                
                res=cur.execute(update, params)
                if res==0:
                    ok=False
                if not self._in_transaction:
                    self.con.commit()
            except mdb.Error as e:
                print("EEEEERRRRRROOOOOOORRRR", e)
                Log.log("Error %d: %s" % (e.args[0],e.args[1]))    
                ok=False
            self.disconnect()
        return ok
    
    def q(self, cad):
        return "'"+(cad)+"'"
    


    def insertListValues(self, table, fields, valuesList):
        for values in valuesList:
            self.insertValues(table, fields, values)
    
    def concatFields(self, fields, isText=False, commas=False):
        first=True
        query=""
        for field in fields:
            if not first:
                query+=","
            first=False
#             if field is None:
#                 query+="NULL"
#            else:
            if (commas):
                field="`"+str(field)+"`"
            if (isText):
                query+=self.q(str(field))
            else:
                query+=str(field)
        return query
    def cleanFields(self, fieldList, isText=False):
        newList=[]
        for field in fieldList:
            f=field
            if isText and field is not None:
                f=self.q(str(field))
            else:
                if field is not None:
                    f=str(field)
            newList.append(f)
        return newList
            
    
    def doInsertQueryMany(self, table, fields, values):
        query ="INSERT INTO "+table+" ("

        query+=self.concatFields(fields, commas=True)
        query+=") VALUES("
                
        query+=", ".join(["%s"]*len(values))
        query+=")"
        print("Q:"+query)
        return query
    
    def insertValues(self, table, fields, values, get_insert_id=False):
        query ="INSERT INTO `"+table+"` ("

        query+=self.concatFields(fields)
        query+=") VALUES("

        query+=self.concatFields(values, True)
        query+=")"
        #print query
        ok, insert_id = self.doUpdate(query, get_insert_id=get_insert_id)
        return ok, insert_id
        
    def insertValuesColumns(self, table, columns_dic, fixedFields={}):
        queryList=[]
        query=""
        count = len(list(columns_dic.values())[0])
        column_keys = list(columns_dic.keys())
        keys = list(fixedFields.keys()) + list(columns_dic.keys())
        for i in range(count):
            values = (list(fixedFields.values()) +
                     [columns_dic[x][i] for x in column_keys])
            if (query==""):
                query=self.doInsertQueryMany(table, keys,values)
            queryList.append(tuple(self.cleanFields(values, False)))
        print ("QueryList", queryList)
        self.doUpdateMany(query, queryList)

        
                
        def insertValuesMany(self, table, dicList):
            queryList=[]
            query=""
#            maxLength=0
            for dic in dicList:
#                print dic.keys()
#                if (maxLength!=0):
#                    if maxLength!=len(dic.values()):
                        
#                        print "problem"
#                        exit(-1)
#                maxLength=max(maxLength, len(dic.values()))
                if (query==""):
                    query=self.doInsertQueryMany(table, list(dic.keys()), list(dic.values()))
                #queryList.append(self.doInsertQueryMany(table, dic.keys(), dic.values()))
                queryList.append(tuple(self.cleanFields(list(dic.values()), False)))
            
            self.doUpdateMany(query, queryList)
            
    
    def getValuesList(self, table, fields, condition="TRUE"):
        query="SELECT "
        query+=self.concatFields(fields)
        query+=" FROM "+table;
        query+=" WHERE "+condition
        #print query
        result=self.doQueryDic(query)
        valuesList=[]
        for row in result:
            values=[]
            for field in fields:
                values.append(row[field])
            valuesList.append(values)
        
        return valuesList
            
            
    def getValuesDicList(self, table, fields, condition="TRUE", orderBy=None):
        rows = []
        query="SELECT "
        query+=self.concatFields(fields)
        query+=" FROM `"+table+"`"
        query+=" WHERE "+condition
        if (orderBy!=None):
            query+=" ORDER BY "+orderBy
               # print query
                #print "QUERY:"+query
        if self.connect():
            try:
                cur=self.con.cursor(mdb.cursors.DictCursor)
                                #print "CUR EXECUTE NEXT"
                cur.execute(query)
                                #print "CUR fetchall NEXT"
                rows=cur.fetchall()    
                cur.close()
                gc.collect()
            except mdb.Error as e:
                Log.log("Error %d: %s" % (e.args[0],e.args[1]))
                rows=False    
            self.disconnect()
        return rows
    
    def getValuesAsColumns(self, table, fields, condition="TRUE", orderBy=None,
                        groupBy=None, no_comma_fields=None, theQuery=None):
        columns={}
        for field in fields:
            columns[field] = []
        
        rows = []
        query="SELECT "
        query+=self.concatFields(fields, commas=True)
        if (no_comma_fields):
            if fields:
                query+=","
            query+=self.concatFields(no_comma_fields, commas=False)
        query+=" FROM "+table;
        if condition!=None:
            query+=" WHERE "+condition
        if (orderBy!=None):
            query+=" ORDER BY "+orderBy
        if (groupBy!=None):
            query+=" GROUP BY "+groupBy
        # print query
        if theQuery:
            query=theQuery
        #print "BIG QUERY:"+query
        if self.connect():
            try:
                cur=self.con.cursor(mdb.cursors.DictCursor)
                cur.execute(query)
                rows=cur.fetchall()    
                for row in rows:
                    for field in fields:
                        columns[field].append(row[field])
                cur.close()
                gc.collect()
            except mdb.Error as e:
                Log.log("Error %d: %s" % (e.args[0],e.args[1]))
                rows=False    
            self.disconnect()
        return columns
    
                
    def getValuesDicList_LowMem(self, table, fields, condition="TRUE", orderBy="None"):
        rows = []
        query="SELECT "
        query+=self.concatFields(fields)
        query+=" FROM "+table;
        query+=" WHERE "+condition
        if (orderBy!=None):
            query+=" ORDER BY "+orderBy
        # print query
        #print "QUERY:"+query
        if self.connect():
            try:
                cur=self.con.cursor(mdb.cursors.DictCursor)
                print("EXECUTE")    
                cur.execute(query)
                print("AFTER EXECUTE")
                return cur
                #rows=cur.fetchall()    
                                #cur.close()
                                #gc.collect()
            except mdb.Error as e:
                Log.log("Error %d: %s" % (e.args[0],e.args[1]))
                rows=False    
            #self.disconnect()
        return None
    
    def close_LowMem(self, cur):
            print("CLOSING ALL SQL ELEMENTS")
            cur.close()
            self.disconnect()
            print("CLOSED ALL SQL ELEMENTS")
#            gc.collect
    @classmethod
    #def copyTable(self, origDb, dstDb,table, fields, condition="TRUE"):
    #    valuesList=origDb.getValuesList(table, fields, condition)
    #    dstDb.insertListValues(table, fields, valuesList)
        
    def copyTable(self, origDb, dstDb,table, fields, condition="TRUE", extraFields=[], extraValues=[]):
        valuesList=origDb.getValuesList(table, fields, condition)
        if (extraFields!=[]):
            fields=fields+extraFields;
            temp=[]
            for values in valuesList:
                values=values+extraValues
                temp.append(values)
            valuesList=temp
            print(fields)
            print(values)
        dstDb.insertListValues(table, fields, valuesList)
        
    def dumpFileOnDB(self, file, table, field, idField, id):
        content=""
        file =openReadFile(file)
        content=file.read()
        file.close()
        query="UPDATE "+table+" SET "+field+"= %s"+" WHERE "+idField+"="+self.q(id)
        print(query)
        return self.doUpdateParams(query, [content])
    
    def setFieldOnTable(self, table, field, fieldValue,idField, idValue, 
                    extra_cond="", no_commas=False):
        values_list=[]
        if no_commas:
            query=("UPDATE "+table+" SET "+field+"= {0}".format(fieldValue)+
               " WHERE "+idField+"="+self.q(idValue)+" "+extra_cond)
        else:
            query=("UPDATE "+table+" SET "+field+"= %s"+
                " WHERE "+idField+"="+self.q(idValue)+" "+extra_cond)
            values_list.append(fieldValue)
        return self.doUpdateParams(query, values_list)
        
    def restoreFieldToFileFromDB(self, file, table, field, idField, id):
        content=self.retoreFieldToStringFromDB(table, field, idField, id)
        if content!="":
            file=openWriteFile(file)
            file.write(content)
            file.close()
            return True
        return False
    
    def retoreFieldToStringFromDB(self, table, field, idField, id):
        query ="SELECT "+field+" FROM "+table+" WHERE "+idField+"="+self.q(id)
        rows = self.doQueryDic(query)
        for row in rows:
            return row[field]
        return ""
            
        
        
        
        