import os

def openReadFile(file):
	try:
		f=open(file, "r")
		
		return f
	except:
		print("Can't open file")
		return 0

def openWriteFile(file):
	try:
		f=open(file, "w")
		
		return f
	except:
		print("Can't open file")
		return 0
	

	
def genCSV(list):
	cad=""
	first=True
	for word in list:
		if (not first):
			cad+=","
		else:
			first=False
		cad+=str(word)
	cad+="\n"
	return cad

def genCSVFile(list, fw):

	first=True
	for word in list:
		cad=""
		if (not first):
			cad+=","
		else:
			first=False
		cad+=str(word)
		fw.write(cad)
	fw.write("\n")
	
		
def ensureDir(directory):
	if not os.path.exists(directory):
		os.makedirs(directory)