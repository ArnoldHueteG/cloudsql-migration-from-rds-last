import json
import sys
import fire
import logging
import multiprocessing
import time
from datetime import datetime
import pickle
from pathlib import Path
from yaml import safe_dump,safe_load
import random
import string
from googleapiclient import discovery, errors
from oauth2client.client import GoogleCredentials

import psycopg2
import os
from gcp import GcpApi

DEFAULT_PORT = 5432
MJ_PREFIX = 'auto-mj-'
CP_SRC_PREFIX = 'src-'

def setup_logger(verbose):
    logger = logging.getLogger(__name__)
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s', datefmt='%Y/%m/%d %H:%M:%S')
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(formatter)
    logger.addHandler(streamHandler)
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    sys.stdout = StreamToLogger(logger, logging.INFO)
    return logger

class StreamToLogger(object):
    """
    Fake file-like stream object that redirects writes to a logger instance.
    """
    def __init__(self, logger, log_level=logging.INFO):
        self.logger = logger
        self.log_level = log_level
        self.linebuf = ''

    def write(self, buf):
        temp_linebuf = self.linebuf + buf
        self.linebuf = ''
        for line in temp_linebuf.splitlines(True):
            # From the io.TextIOWrapper docs:
            #   On output, if newline is None, any '\n' characters written
            #   are translated to the system default line separator.
            # By default sys.stdout.write() expects '\n' newlines and then
            # translates them so this is still cross platform.
            if line[-1] == '\n':
                self.logger.log(self.log_level, line.rstrip())
            else:
                self.linebuf += line

    def flush(self):
        if self.linebuf != '':
            self.logger.log(self.log_level, self.linebuf.rstrip())
        self.linebuf = ''

class DataMigrationService:
    def __init__(self,config="config.yaml",verbose=False):
        """[summary]"""
        self._config=config
        self._logger = setup_logger(verbose)
        self._gcp = GcpApi(logger=self._logger)
        self._now_str = datetime.now().strftime("%Y%m%dt%H%M%S")
        with open(self._config) as f:
            self._db_config = safe_load(f)
        #self.rds_name = source_connection["postgresql"]["host"].split(".")[0]
    
    def get_progress(self, dbname):
        """
        get progress of migration
        """
        cfg = self._db_config[dbname]
        from get_metadata import get_percentage_migrated
        if self.test_connection(dbname):
            progress = get_percentage_migrated( f'{cfg["aws-host"]}:{cfg["aws-port"]}:{cfg["aws-replication-username"]}:{cfg["aws-replication-password"]}',
                                        f'{cfg["gcp-host"]}:{cfg["gcp-port"]}:{"postgres"}:{cfg["gcp-root-password"]}')
        else:
            progress = 0
        self._logger.info(f"progress : {progress}%")

    def sync(self, dbname):
        """
        Starts db migration process.
        1. Creates and starts migration job
        :param dbname: name of service in the config yaml
        """
        self._logger.info("Starting migration job")
        if not self.test_connection(dbname):
            self._logger.info(
                "migration job won't continue because connection test was not succesful"
            )
            return
        cfg = self._db_config[dbname]

        # Prepare migration job and Start
        self._create_connection_profile(dbname)
        self._create_dms_job(dbname)

        # Create cloudsql users and Retrieve cloudsql information
        self._await_state(dbname, "RUNNING")
        self._logger.info("job running, await database CDC phase")
        self._await_phase(dbname, target_phase="CDC")
        self._logger.info("CDC phase reached, sync complete, ready to cutover")

    def _save_db_config(self):
        """
        Save config dict to configuration  yaml file.
        """
        with open(self._config, 'w') as f:
            safe_dump(self._db_config, f, sort_keys=False)
    
    def _await_state(self, dbname, target_state):
        """
        Await a state of job
        https://cloud.google.com/database-migration/docs/reference/rest/v1alpha2/projects.locations.migrationJobs#phase
        :param dbname:
        :param target_state:
        :return:
        """
        job_desc = self._describe_dms_job(dbname)
        if job_desc["state"] is None:
            raise Exception(f"job was not found")

        current_state = job_desc['state']
        sleep_time = 1
        self._logger.info(f"state of job/{dbname}: {current_state}, target: {target_state}")
        while current_state != target_state:
            time.sleep(sleep_time)
            sleep_time = min(10, sleep_time * 2)
            job_desc = self._describe_dms_job(dbname)
            if job_desc['state'] == 'FAILED':
                raise Exception(f"job failed: {job_desc}")
            else:
                current_state = job_desc['state']
        self._logger.info(f"state of job/{dbname}: {job_desc}")

    def _await_phase(self, dbname, target_phase="CDC"):
        """
        Await a phase of data transfer. Note that the STATE of the job must be RUNNING!!!
        https://cloud.google.com/database-migration/docs/reference/rest/v1alpha2/projects.locations.migrationJobs#phase
        :param dbname:
        :param target_phase:
        :return:
        """
        phases = {'PHASE_UNSPECIFIED': 1000, 'FULL_DUMP': 2, 'CDC': 3, 'PROMOTE_IN_PROGRESS': 4}
        job_desc = self._describe_dms_job(dbname)
        if job_desc["state"] != "RUNNING":
            raise Exception(f"job was not in RUNNING state: {job_desc}")

        current_phase = job_desc['phase']
        sleep_time = 1
        self._logger.info(f"phase {dbname}: {current_phase}, target: {target_phase}")
        while phases.get(current_phase, -1) < phases.get(target_phase, -2):
            time.sleep(sleep_time)
            sleep_time = min(10, sleep_time * 2)
            job_desc = self._describe_dms_job(dbname)
            if job_desc['state'] == 'COMPLETED':
                break
            elif job_desc['state'] != 'RUNNING':
                raise Exception(f"job was not in RUNNING state: {job_desc}")
            else:
                current_phase = job_desc['phase']
        self._logger.info(f"phase {dbname}: {job_desc}, target: {target_phase}")
        
    def _create_dms_job(self, dbname=None):
        """
        Creates Database Migration Service Job for a particular database
        Defaults to None.

        Creating dms job does the following steps:
        1. Create dms job
        2. Start dms job
        :param dbname: name of database in the config yaml
        """
        self._logger.info(f"Creating Database Migration Service job for {dbname}")
        connection_profile_id_source = f"{CP_SRC_PREFIX}{dbname}"
        connection_profile_id_destination = f"sql-{dbname}-{self._now_str}"
        migration_job_id = "{}{}".format(MJ_PREFIX, dbname)
        config = self._db_config[dbname]
        project_id = config.get("gcp-project-id")
        region_id = config.get("gcp-instance-region")

        request_body = {
            "type": "CONTINUOUS",
            "source": f"projects/{project_id}/locations/{region_id}/connectionProfiles/{connection_profile_id_source}",
            "destination": f"projects/{project_id}/locations/{region_id}/connectionProfiles/{connection_profile_id_destination}",
            "destinationDatabase": {
                "provider": "CLOUDSQL",
                "engine": "POSTGRESQL"
            }
        }
        self._gcp.create_migration_job(project_id, region_id, migration_job_id, request_body)
        self._gcp.start_migration_job(project_id, region_id, migration_job_id)

    def _create_connection_profile(self, dbname=None):
        """
        Creates required connection profiles for enabling migration job for a
        particular database.Defaults to None.
        Creating conection profiles does the following steps:
        0. Checks if connection profile exists. If there is, then updates the connection profile with new info.
        1. Creates source "postgresql" connection profile (AWS)
        2. Creates destination "cloudsql" connection profile
        :param dbname: name of database in the config yaml
        """

        self._logger.info(f"creating connection profiles for {dbname}")
        config = self._db_config.get(dbname)
        project_id = config.get("gcp-project-id")
        region_id = config.get("gcp-instance-region")
        migration_job_id = f"{MJ_PREFIX}{dbname}"

        # create source connection profile
        connection_profile_id_aws = f"{CP_SRC_PREFIX}{dbname}"
        request_body_aws = {
            "displayName": connection_profile_id_aws,
            "postgresql": {
                "host": config["aws-host"],
                "port": config["aws-port"],
                "username": config["aws-replication-username"],
                "password": config["aws-replication-password"]
            }
        }
        self._gcp.upsert_connection_profile(project_id, region_id, connection_profile_id_aws, request_body_aws)

        # create dest, if the cloudsql instance name exists
        connection_profile_id_gcp = self._gcp.get_cloudsql_instance_name(project_id, region_id, migration_job_id)
        if connection_profile_id_gcp is not None:
            self._logger.info(f"cloud SQL destination instance for {dbname} already created: {connection_profile_id_gcp}")
            return None

        # must create dest cloudsql instance, and save its root password
        connection_profile_id_gcp = f"sql-{dbname}-{self._now_str}"
        cloudsql_root_password = ''.join(
            random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(12))
        gcp_cpu = config["gcp-instance-cpu"]
        gcp_mem = config["gcp-instance-mem"]
        self._logger.debug(f"{connection_profile_id_gcp} cpu: {gcp_cpu}, mem: {gcp_mem}")
        request_body_cloudsql = {
            "displayName": connection_profile_id_gcp,
            "cloudsql": {
                "settings": {
                    "autoStorageIncrease": config["gcp-auto-storage-increase"],
                    "dataDiskType": config["gcp-disk-type"],
                    "rootPassword": cloudsql_root_password,
                    "databaseVersion": config["gcp-database-version"],
                    "tier": f"db-custom-{gcp_cpu}-{gcp_mem}",
                    "dataDiskSizeGb": config["gcp-instance-storage"],
                    "sourceId": f"projects/{project_id}/locations/{region_id}/connectionProfiles/{connection_profile_id_aws}",
                    "ipConfig": config["gcp-ip-config"]
                }
            }
        }
        self._gcp.upsert_connection_profile(project_id, region_id, connection_profile_id_gcp, request_body_cloudsql)
        cloudsql_host = self._gcp.get_cloudsql_host(project_id, connection_profile_id_gcp)
        self._logger.debug(f"host for {dbname}/{connection_profile_id_gcp}: {cloudsql_host}")
        self._logger.debug(f"root_password for {dbname}/{connection_profile_id_gcp}: {cloudsql_root_password}")
        self._db_config[dbname]["gcp-root-password"] = cloudsql_root_password
        self._db_config[dbname]["gcp-host"] = cloudsql_host
        self._save_db_config()

    def _describe_dms_job(self, dbname):
        """
        Describes current phase of Database Migration Job for a particular database.

        :param dbname: name of database in the config yaml
        :return {state:, status:, error:} or None if the job was not found
        """
        cfg = self._db_config[dbname]
        return self._gcp.get_dms_status(cfg["gcp-project-id"], cfg["gcp-instance-region"], f"{MJ_PREFIX}{dbname}")

    def cleanup(self, dbname):
        """
        Delete the completed job associated with a database. Also deletes any content associated with it
        such as connection profiles
        :param database:
        :return:
        """
        cfg = self._db_config[dbname]
        project_id = cfg["gcp-project-id"]
        region = cfg["gcp-instance-region"]
        job_id = f"{MJ_PREFIX}{dbname}"
        job_state = self._gcp.get_dms_status(project_id, region, job_id)
        if not job_state:
            self._logger.warning(f"job for service {dbname} was not found, exiting")
            return

        job_state = job_state['body']
        aws_ref_instance = job_state['destination'].split("/")[-1] + "-master"
        try:
            self._logger.info(f"deleting db ref {aws_ref_instance}")
            self._gcp.delete_cloudsql_instance(project_id, aws_ref_instance)
        except Exception as e:
            self._logger.warning(f"unable to delete sql instance '{aws_ref_instance}'. {str(e)}")

        try:
            self._logger.info(f"deleting profile {job_state['source']}")
            self._gcp.delete_dms_connection_profile(job_state['source'])
        except Exception as e:
            self._logger.warning(f"unable to delete source connection profile '{job_state['source']}'. {str(e)}")

        try:
            self._logger.info(f"deleting job {job_id}")
            self._gcp.delete_dms_job(project_id, region, job_id)
        except Exception as e:
            self._logger.warning(f"unable to delete dms job {job_id}. {str(e)}")

    def test_connection(self,dbname):
        """
        Test connection to database. Returns true if successful, false otherwise.
        :param database:
        :return:
        """
        dc = self._db_config[dbname]
        try:
            self._logger.info("Testing connection")
            conn = psycopg2.connect(
                dbname="postgres",
                host=dc["aws-host"],
                user=dc["aws-replication-username"],
                password=dc["aws-replication-password"],
                port=dc["aws-port"],
            )
            conn.close()
            self._logger.info(
                "Your connection to {} was successful.".format(
                    dc["aws-host"]
                )
            )
            return True
        except Exception as e:
            self._logger.info(
                "Your connection to {} was not successful.".format(
                    dc["aws-host"]
                )
            )
            self._logger.debug("{}".format(e).strip())
            return False

if __name__ == '__main__':
    fire.Fire(DataMigrationService)