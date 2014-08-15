#!/usr/local/bin/python

from __future__ import print_function,with_statement

from fabric.api import run,env,sudo,execute,local
from fabric.decorators import *
from fabric.context_managers import *
from pyzabbix import ZabbixAPI

import requests, argparse, ConfigParser,os

ZABBIX_SERVER = "http://10.194.28.156/zabbix"
zapi = ZabbixAPI(ZABBIX_SERVER)
zapi.login('rpc', 'mypna123')

env.skip_bad_hosts = True
env.keepalive = 60

#Recycle server
#recycle = "eq2-vmrecycle-01.prod.mypna.com" #should modify to new EC2 nfs server

#Gobal variable
archive = ''
directories = ''
host_string = ''
cluster_name = ''
all_catalina_dir = ''
all_userlog_dir = ''

all_log_dir = []
loguser = []

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    BOLD = "\033[1m"
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'

    def disable(self):
        self.HEADER = ''
        self.OKBLUE = ''
        self.OKGREEN = ''
        self.BOLD = ""
        self.WARNING = ''
        self.FAIL = ''
        self.ENDC = ''

def make_up_full_dns(host):
	hostname_short = host.split('.')[0]
	try:
		host_split = hostname_short.split('-')
		if host_split[0] == 'ec1':
			return hostname_short + '.ec1.mypna.com'
		elif host_split[0] == 'ec2':
			return hostname_short + '.ec2.mypna.com'
		elif host_split[0].startswith('eq'):
			return hostname_short + '.prod.mypna.com'
	except:
		print("Sorry, this script can only add EQX and AWS hosts!")

def short_dns(host_string):
	return host_string.split('.')[0]

#Mark IRC message
def irc_mark(nickname,message):
	url = '''http://telenav-irc.telenav.com:8081/IRC_Requests/?nick=''' + nickname + '''&msg=''' + message
	r = requests.get(url)

def mountNFS():
	run('mkdir -p /tmp/logTmp/')
	logTmp = '/tmp/logTmp/'
	if os.path.isdir(logTmp):
		try:
			run('sudo mount ec2-sgnfs-01.ec2.mypna.com:/vol1/noc /tmp/logTmp/')
			if run('mount|grep ec2-sgnfs-01'):
				pass
			else:
				print 'ec2-sgnfs-01 is not avaiable'
		except 
			print("error")
			sys.exit(0)
	arichive = "/tmp/logTmp/"

def umountNFS():
	run('cd /')
	try:
		run('sudo umount /tmp/logTmp')
	except Exception,e:
		print Exception.":",e
		sys.exit(0)

#@hosts(recycle)							#from fabric.decorators.hosts,point run command on recycle
#def data_vmrecycle():
#	global archive
#	output=run(''' df -Ph|awk '$6 ~ "/data" {print $5$6}' ''')
	#output
	#75%/data/1
	#36%/data/2
	#23%/data/3
#	data = [ i.split('%') for i in output.splitlines() ]
#	for percent,data_dir in data:
#		dest_root = ''
#		if float(percent) < 90:
#			dest_root = data_dir
#			break
#	if dest_root=='':
#		print("Disk is almost full on " + recycle + ".")
#		sys.exit(0)
#
#	archive = run("find " + dest_root + ''' -type d -name '*[aA]rchive*' -print 2>/dev/null || exit 0''')



#@hosts(recycle)
def make_recycle_dir():
	for path in directories:
		command = ''' [ -w ''' + path + ''' ] && echo True || echo False'''
		flag = run(command)
		if flag=='True':
			pass
		else:
			command = ''' [ -d ''' + path + ''' ] && sudo chmod 777 ''' + path + ''' || sudo mkdir -p -m777 ''' + path
			run(command)

#The java user.
def log_user():
	java_user=run("ps -ef|grep java|grep -v `whoami`|awk '{print $1}'")
	return list(set(java_user.split('\r\n')))

def catalina_home():
	command='''ps -fC java --noheaders|awk '{for (i=1;i<=NF;i++) { if ( $i ~ /Dcatalina.home/ ) {split($i,x,"="); print x[2]}}}' '''
	return run(command).splitlines()

def home_log_dir(user):
	path = "/home/" + user +"/"
	command = "find " + path + " \( -type d -o -type l \) -name '*log*' -print 2>/dev/null"
	return sudo(command,user=user).splitlines()

#All log dirs that exist on the target host.
def all_log_directories(mtime):
	global loguser, all_catalina_dir, all_userlog_dir, all_log_dir
	loguser = log_user()
	all_log_dir = []
	for each_dir in catalina_home():
		all_catalina_dir += " " + each_dir + "/logs/"
	for each_user in loguser:
		userid = run("id -u " + each_user)
		for each_dir in home_log_dir(each_user):
			all_userlog_dir = all_userlog_dir + " " + each_dir + "/"
		command_b = "find " + all_userlog_dir + " " + all_catalina_dir + " -maxdepth 3 -type f \( -name '*.gz' -a -name '*-??-*' \) -mtime +" + mtime + " -user " + userid + ''' -print|awk -F/ 'BEGIN{OFS="/"}{$NF="";print}'|uniq '''
		all_log_dir.extend(run(command_b).splitlines())
	all_log_dir = list(set(all_log_dir))

#Create the file dir on vmrecycle.
def file_directories_to_built():
	global directories, cluster_name, host_string
#	print(bcolors.OKBLUE + "Archiving the *.gz files from " + env.host_string + " to " + archive.splitlines()[0] + " on " + recycle + "..." + bcolors.ENDC)
	cluster_name = local('echo ' + env.host_string + ''' |awk -F- '{print $2}' ''', capture=True)
	host_string = local('echo ' + env.host_string + ''' |awk -F. '{print $1}' ''', capture=True)
	directories = [(archive.splitlines()[0] + "/" + cluster_name + "/" + host_string + each_iteam) for each_iteam in all_log_dir]

#Retrieve hostgroup from ZABBIX.
def retrieve_hostgroup(group_name_list):
	groupids = zapi.hostgroup.get(
		output=['goupid'],
		filter={
			'name':group_name_list
		}
	)
	return [ each_item['groupid'] for each_item in groupids ]

#Retrieve hosts from ZABBIX.
def retrieve_host_with_groupid(groupids, cluster_name):
	hosts=zapi.host.get(
		output=['host'],
		groupids=groupids,
		filter={
			'status':0 #Select the hosts status: monitored
		},
		search={
			'host':'-' + cluster_name + '-'
		}
	)
	return [ each['host'] for each in hosts ]

#Retrieve hosts from ZABBIX.
def get_cluster_instances(cluster_name):
	groupids = retrieve_hostgroup(['EC1-SOE', 'EC2-SOE'])
	filtered_hosts = retrieve_host_with_groupid(groupids, cluster_name)
	return filtered_hosts

#Fab transfer job.
def transfer_function(mtime, bwlimit):
	#Refresh the value of the global variable
	global archive, directories, host_string, cluster_name, all_catalina_dir, all_userlog_dir, all_log_dir, log_user
	archive = ''
	directories = ''
	host_string = ''
	cluster_name = ''
	all_catalina_dir = ''
	all_userlog_dir = ''
	
	all_log_dir = []
	loguser = []

	print("*************************************************************")
	print(bcolors.HEADER + "Working on " + env.host_string + "..." + bcolors.ENDC)
#	with quiet():
#		execute(data_vmrecycle)
	all_log_directories(mtime)
	file_directories_to_built()
	execute(mountNFS)
	print(bcolors.OKGREEN + "Making directories: " + str(directories) + ", if they doesn't exist." + bcolors.ENDC)
	execute(make_recycle_dir)
	print("Transferring the *.gz files...")
	irc_mark('LogTransfer','Start log transfer: from ' + short_dns(env.host_string) + ' to vmrecycle, use rsync --bwlimit=' + bwlimit)
	for each_user in log_user():
		with quiet():
			userid = run("id -u " + each_user)
			command = "find " + all_userlog_dir + " " + all_catalina_dir + " -maxdepth 3 -type f -name '*.gz' -mtime +" + mtime + " -user " + userid + ''' -print '''
			x = run(command).splitlines()
		x = sorted(set(x))
		for each_local_file in x:
			with quiet():
				tmp = local("echo " + each_local_file + ''' |awk -F/ 'BEGIN{OFS="/"}{$NF="";print}' ''', capture=True)
			remote_path = archive.splitlines()[0] + "/" + cluster_name + "/" + host_string + tmp
			print('###File ' + bcolors.BOLD + each_local_file + bcolors.ENDC + ' from ' + bcolors.BOLD + host_string + bcolors.ENDC + ' in progress.###' )
			with quiet():
				command = ''' rsync -avRh --remove-sent-files --ignore-errors --bwlimit=''' + bwlimit + ' ' + each_local_file + ' ' +  remote_path
				sudo(command,user=each_user)

	execute(umountNFS)
	irc_mark('LogTransfer','End log transfer: from ' + short_dns(env.host_string) + ' to NFS server')
	print(bcolors.BOLD + "Done for " + bcolors.OKGREEN + host_string + bcolors.ENDC + "!" + bcolors.ENDC)
	
def job_for_section(config, section, mtime, bwlimit):
	for each_cluster in config.options(section):
		hosts = []
		hosts.extend(get_cluster_instances(each_cluster))
		env.hosts = [ make_up_full_dns(each) for each in hosts ]
		if env.hosts:
			execute(transfer_function, mtime, bwlimit)

if __name__ == '__main__':
	parser = argparse.ArgumentParser(
		description='Transfer .gz log file from TeleNav AWS SOE servers to S3 based  NFS server.'
		)
	parser.add_argument(
		'-d',
		type=int,
		default=270,
		metavar='Num',
		dest='mtime',
		help='Specify the .gz file of how many days ago will be Transferred.'
		)
	parser.add_argument(
		'--bwlimit',
		type=int,
		default=5000,
		metavar='Num',
		dest='bwlimit',
		help='Specify the --bwlmit that used by rsync.'
		)
	parser.add_argument(
		'hosts',
		type=str,
		nargs='*',
		help='Hosts on which .gz log file will be transferred.'
		)
	parser.add_argument(
		'-f',
		'--config-file',
		nargs='?',
		dest='config_file',
		type=argparse.FileType('r'),
		help='Config file for this script, use config file will ignore the other arguments.'
		)
	args = parser.parse_args()

	if args.config_file:
		config = ConfigParser.ConfigParser(allow_no_value=True)
		config.readfp(args.config_file)
		job_for_section(config, 'EC_cluster_3_7_days', '5', '5000')
		job_for_section(config, 'EC_cluster_7_30_days', '30', '5000')
		job_for_section(config, 'EC_cluster_30_60_days', '60', '5000')
		#irc_mark('LogTransfer','[COMPLETED] Daily log transfer for today.')
	elif args.hosts:
		env.hosts = [ make_up_full_dns(each) for each in args.hosts ]
		mtime = str(args.mtime)
		bwlimit = str(args.bwlimit)
		execute(transfer_function, mtime, bwlimit)
	else:
		parser.print_help()
