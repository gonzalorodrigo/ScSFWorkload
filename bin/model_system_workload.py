"""
This scripts is an example of how to model the workload of a system. It
reads a job log trace from a database and saves the model.

If the origin data format is not in a database or a different database scheme,
load_from_db should be re-defined. Saves the model in the data folder.

Parameters:
- Env vars: NERSCDB_USER, NERSCDB_PASS contain the username and password to
  to connect to the db.
- if db_is_local, database is assumed to be local on a sstandard port. if not
  db_is_local db port is 5050, meant to be a ssh-fwd to a remote db.
 
Trace Database is suposed to have the following schema (torque db format):
 
 CREATE TABLE `summary` (
  `stepid` varchar(64) NOT NULL DEFAULT '',
  `jobname` varchar(64) DEFAULT NULL,
  `owner` varchar(8) DEFAULT NULL,
  `account` varchar(8) DEFAULT NULL,
  `jobtype` varchar(32) DEFAULT NULL,
  `cores_per_node` smallint(6) DEFAULT NULL,
  `numnodes` int(11) DEFAULT '1',
  `class` varchar(64) DEFAULT NULL,
  `status` varchar(64) DEFAULT NULL,
  `dispatch` bigint(20) DEFAULT NULL,
  `start` bigint(20) DEFAULT NULL,
  `completion` bigint(20) NOT NULL DEFAULT '0',
  `queued` bigint(20) DEFAULT NULL,
  `wallclock` bigint(20) DEFAULT NULL,
  `mpp_secs` bigint(20) DEFAULT NULL,
  `wait_secs` bigint(20) DEFAULT NULL,
  `raw_secs` bigint(20) DEFAULT NULL,
  `superclass` varchar(64) DEFAULT NULL,
  `wallclock_requested` bigint(20) DEFAULT '0',
  `hostname` varchar(16) NOT NULL DEFAULT 'franklin',
  `memory` bigint(20) DEFAULT '0',
  `created` bigint(20) DEFAULT '0',
  `refund` char(1) DEFAULT '',
  `tasks_per_node` int(11) DEFAULT '0',
  `vmemory` bigint(20) DEFAULT '0',
  `nodetype` varchar(20) DEFAULT '',
  `classG` varchar(64) DEFAULT NULL,
  `filtered` tinyint(1) DEFAULT '0',
  PRIMARY KEY (`stepid`,`completion`),
  KEY `owner` (`owner`,`completion`),
  KEY `account-completion` (`account`,`completion`),
  KEY `topsearch` (`completion`,`owner`),
  KEY `hostname-completion` (`hostname`,`completion`),
  KEY `hostname` (`hostname`,`owner`),
  KEY `starthost` (`start`,`hostname`),
  KEY `start` (`start`),
  KEY `startASChostfilter` (`start`,`hostname`,`filtered`) USING BTREE,
  KEY `created` (`created`),
  KEY `createdASChostfilter` (`created`,`hostname`,`filtered`),
  KEY `hostnameAlone` (`hostname`),
  KEY `queuedTime` (`queued`,`hostname`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 

"""
import datetime

from machines import Edison

db_is_local=True

# Machine class. Edison
edison = Edison()

start=datetime.date(2015, 1, 1)
end=datetime.date(2015, 12, 31)
print("Loading workload trace and generating model...")
edison.load_from_db(start, end, db_is_local)
print("Saving model...")
# Will save the model as 2015-edison
edison.save_to_file("./data", "2015")
print("DONE")