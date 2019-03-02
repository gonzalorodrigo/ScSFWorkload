from filemanager import *
import datetime
import multiprocessing as mp

class Log:
	
	logFileRoute="log.log"
	logFile=""
	
	@classmethod
	def setLogFile(self, route):
		self.closeLogFile()
		self.logFileRoute=route
		self.logFile=openWriteFile(self.logFileRoute)
	@classmethod
	def closeLogFile(self):
		if (self.logFile!=""):
			self.logFile.close()
	@classmethod		
	def log(self, msg):
		print msg
		if (self.logFile==""):
			return
		cad=datetime.datetime.now().isoformat("-")
		cad+=":"+str(mp.current_process().pid)
		cad+=":"+msg
		#print "I'm going to write on:"+str(self.logFile)+":"+str(msg)
		self.logFile.write(str(cad)+"\n")
		self.logFile.flush()
		
		
		