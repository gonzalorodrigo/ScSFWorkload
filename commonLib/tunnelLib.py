import subprocess 
import time
class Tunnel:


    def __init__(self, route=None):
        if route!=None:
            self.route=route
        else:
            self.route="/usr/bin/ssh -L 5050:127.0.0.1:3306 gonzalo@underdog.lbl.gov -N"
        self.shell="/bin/sh"
        self.connected=False
    
    def connect(self):
        print "Creating tunnel"
        if (not self.connected):
    #        subprocess.call([self.shell, self.route, "&"])
            p = subprocess.Popen(self.route.split())
            time.sleep(5)
            self.subP=p
            print "Tunnel Started, pid:", self.subP.pid
            self.connected=True
        else:
            print "Tunnel already created"
    
    def disconnect(self):
        if (self.connected):
            self.subP.kill()
            time.sleep(5)
            print "Tunnel Destroyed"
        else:
            print "Tunnel didn't exist"
