"""Implementation of the sremote.api.ClientChannel class that uses SSH as
the communication channel. It uses scp and direct ssh execution."""



import subprocess
from getpass import getuser

class SSH(object):

    def __init__(self, hostname, username=None, password=None):
        self._hostname = hostname;
        self._username=username
        if self._username is None:
            self._username = getuser()
 
    def push_file(self, origin_route, dest_route):
        command_list =  ["scp", origin_route, self._username + "@" +
                         self._hostname + ":" + dest_route]
        #print command_list
        p = subprocess.Popen(command_list, stdout=subprocess.PIPE)
        output, err = p.communicate()
        rc = p.returncode
        if (rc!=0):
            print "File push operation error", output, err
        return rc == 0
    
    def retrieve_file(self, origin_route, dest_route):
        command_list =  ["scp", self._username + "@" +
                 self._hostname + ":" + origin_route,  dest_route]
        #print command_list
        p = subprocess.Popen(command_list, stdout=subprocess.PIPE)
        output, err = p.communicate()
        rc = p.returncode
        if (rc!=0):
            print "File retrieve operation error", output, err
        return rc == 0
    
    def delete_file(self, route):
        output, err, rc=self.execute_command("/bin/rm", [route])
        if rc!=0:
            print "File delete operation error", output, err
        return rc==0
        
        
    def execute_command(self, command, arg_list=[], keep_env=False,
                        background=False):
        output=None
        err=""
        if background:
            command_list = ["ssh", self._username+"@"+self._hostname, 
                        "nohup", command] + arg_list
            p = subprocess.Popen(command_list)
        else:
            command_list = ["ssh", self._username+"@"+self._hostname, 
                        command] + arg_list
                        
            p = subprocess.Popen(command_list, stdout=subprocess.PIPE)
            output, err = p.communicate()
        if err is None:
            err=""
        rc = p.returncode
        return output, err, rc
    
    def get_home_dir(self):
        return self._home_dir
    

