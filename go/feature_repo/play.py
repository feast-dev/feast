import psutil
import re

for proc in psutil.process_iter():
   try:
       # Fetch process details as dict
       pinfo = proc.as_dict(attrs=['pid', 'name', 'username'])
       # p = re.compile('*goserver')
       if 'goserver' in pinfo['name']:
           print(pinfo)
   except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
       pass
