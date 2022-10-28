import logging
import random
import string
import time

from googleapiclient import discovery
from googleapiclient.errors import HttpError


class GcpApi:
    def __init__(self, logger=None):
        self._dms_api = None
        self._sqladmin_api = None
        self._resource_manager_api = None
        self._projects_cache = None
        
        self._logger = logging.getLogger(__name__) if not logger else logger

    def dms(self):
        if self._dms_api is None:
            self._dms_api = discovery.build('datamigration', 'v1')
        return self._dms_api

    def sqladmin(self):
        if self._sqladmin_api is None:
            self._sqladmin_api = discovery.build('sqladmin', 'v1beta4')
        return self._sqladmin_api

    def resource_api(self):
        if self._resource_manager_api is None:
            self._resource_manager_api = discovery.build('cloudresourcemanager', 'v1')
        return self._resource_manager_api

    def get_dms_status(self, project_id, region_id, migration_job_id):
        """
        :param project_id:
        :param region_id:
        :param migration_job_id:
        :return: None if not found. dict of state, status, and error (if error was present)
        """
        try:
            body = self.dms().projects().locations().migrationJobs().get(
                name=f"projects/{project_id}/locations/{region_id}/migrationJobs/{migration_job_id}").execute()
            res = {
                "state": body.get("state"),
                "phase": body.get("phase"),
                "error": body.get("error", None),
            }
            self._logger.info(f"migration job state for {project_id}/{migration_job_id}: {res}")
            res['body'] = body
            return res
        except Exception as error:
            self._logger.warning(f"failed to get migration job for {project_id}/{migration_job_id}: {error}")
            return None

    def promote_dms_job(self, project_id, region_id, migration_job_id):
        try:
            self.dms().projects().locations().migrationJobs().promote(
                name=f"projects/{project_id}/locations/{region_id}/migrationJobs/{migration_job_id}").execute()
        except Exception as error:
            self._logger.warning(f"failed to promote dms job {project_id}/{migration_job_id}: {error}")
            raise error

        if self._logger.isEnabledFor(logging.DEBUG):
            # side effect: logs the state of the DMS job
            self.get_dms_status(project_id, region_id, migration_job_id)

    def delete_dms_job(self, project_id, region_id, migration_job_id):
        """
        :return: Operation object
        """
        name = f"projects/{project_id}/locations/{region_id}/migrationJobs/{migration_job_id}"
        op = self.dms().projects().locations().migrationJobs().delete(name=name).execute()
        self._await_operation(lambda: self.dms().projects().locations().operations().get(name=op['name']).execute())

    def delete_dms_connection_profile(self, name):
        """
        :param name:  projects/{projectId}/locations/{region}/connectionProfiles/{name}
        """
        op = self.dms().projects().locations().connectionProfiles().delete(name=name).execute()
        self._await_operation(lambda: self.dms().projects().locations().operations().get(name=op['name']).execute())

    def get_cloudsql_instance_name(self, project_id=None, region_id=None, migration_job_id=None):
        """
        :return: cloudSQL instance name for job or None if not exists
        """
        try:
            response = self.dms().projects().locations(). \
                migrationJobs().get(
                name=f"projects/{project_id}/locations/{region_id}/migrationJobs/{migration_job_id}").execute()
            return response["destination"].split("/")[-1]
        except Exception as error:
            #self._logger.debug(f"failed to get gcp instance name for dms job {project_id}/{migration_job_id}, {error}")
            return None

    def check_connection_profile_state(self, project_id, region_id, connection_profile_id):
        profile_path = f"projects/{project_id}/locations/{region_id}/connectionProfiles/{connection_profile_id}"
        try:
            response = self.dms().projects().locations().connectionProfiles().get(name=profile_path).execute()
            return response.get("state")
        except HttpError as error:
            pass
            #self._logger.debug(f"failed to check for connectionProfile {profile_path}: {error.status_code}")
        except Exception as error:
            pass
            #self._logger.debug(f"failed to check for connectionProfile {profile_path}")
        return 'NOT_EXISTS'

    def upsert_connection_profile(self, project_id, region_id, connection_profile_id, request_body):
        """
        :param project_id:
        :param region_id:
        :param connection_profile_id:
        :param request_body:
        :return: 'UPDATE' if updated otherwise 'CREATE' if created
        """
        profile_path = f"projects/{project_id}/locations/{region_id}/connectionProfiles/{connection_profile_id}"

        # try update if exists
        if self.check_connection_profile_state(project_id, region_id, connection_profile_id) != 'NOT_EXISTS':
            self._logger.info("connection profile is going to be updated")
            update_mask = "postgresql.host,postgresql.port,postgresql.username,postgresql.password"
            self.dms().projects().locations().connectionProfiles().patch(name=profile_path, updateMask=update_mask,
                                                                         body=request_body).execute()
        else:
            self._logger.info("connection profile is going to be created")
            try:
                self.dms().projects().locations().connectionProfiles().create(
                    parent=f"projects/{project_id}/locations/{region_id}",
                    connectionProfileId=connection_profile_id,
                    body=request_body).execute()
                self._logger.info(f"await connection profile {connection_profile_id} to be READY")

                sleep_time = 0.1
                state = None
                while state != "READY":
                    state = self.check_connection_profile_state(project_id, region_id, connection_profile_id)
                    self._logger.debug(f'await connection profile {connection_profile_id} to be READY. Current: {state}')
                    time.sleep(sleep_time)
                    sleep_time = min(sleep_time * 2, 10)

                self._logger.info(f"connection profile {connection_profile_id} is READY")
            except Exception as error:
                raise Exception(f"failed to create connection profile for {connection_profile_id}", error)

    def create_migration_job(self, project_id, region_id, migration_job_id, request_body):
        dms_job_path = f"projects/{project_id}/locations/{region_id}/migrationJobs/{migration_job_id}"
        try:
            self.dms().projects().locations().migrationJobs().get(name=dms_job_path).execute()
        except:
            try:
                self.dms().projects().locations().migrationJobs().create(
                    parent="projects/{}/locations/{}".format(project_id, region_id),
                    migrationJobId=migration_job_id,
                    body=request_body).execute()
                self._logger.info("Waiting for DMS Job: {} to be READY".format(migration_job_id))
                sleep_time = 0.1
                state = ''
                while state != 'NOT_STARTED':
                    time.sleep(sleep_time)
                    sleep_time = min(1, sleep_time * 2)
                    response = self.dms().projects().locations().migrationJobs().get(name=dms_job_path).execute()
                    state = response.get('state')
            except Exception as error:
                raise Exception("Cannot CREATE migration job for {}: {}".format(dms_job_path, error))

    def start_migration_job(self, project_id, region_id, migration_job_id):
        dms_job_path = f"projects/{project_id}/locations/{region_id}/migrationJobs/{migration_job_id}"
        try:
            sleep_time = 0.1
            state = ''
            while state != 'RUNNING':
                response = self.dms().projects().locations().migrationJobs().get(name=dms_job_path).execute()
                state = response.get("state")
                # see: https://cloud.google.com/database-migration/docs/reference/rest/v1/projects.locations.migrationJobs#State
                if state == "NOT_STARTED":
                    self.dms().projects().locations().migrationJobs().start(name=dms_job_path).execute()
                    self._logger.info(f"Started DMS Job: {migration_job_id}, await RUNNING")
                elif state == 'FAILED':
                    raise Exception(f"failed start migration job: '{response.get('error').get('message')}'")
                elif state == "COMPLETED":
                    raise Exception(f"failed start migration job: already completed")
                sleep_time = min(1, sleep_time * 2)
                time.sleep(sleep_time)

            self._logger.info("DMS Job: {} is RUNNING".format(migration_job_id))
        except Exception as error:
            raise Exception("Cannot START migration job for {}: {}".format(dms_job_path, error))

    def delete_cloudsql_instance(self, project_id, instance):
        op = self.sqladmin().instances().delete(project=project_id, instance=instance).execute()
        self._await_operation(lambda: self.sqladmin().operations().get(project=project_id, operation=op['name']).execute())

    def create_cloudsql_user(self, project_id, instance, username, password=None):
        """
        :return: password for cloudsql user if not already passed in
        """
        if password is None:
            password = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(12))
        self.sqladmin().users().insert(project=project_id, instance=instance,
                                       body={"name": username, "password": password}).execute()
        return password

    def get_cloudsql_host(self, project=None, instance=None):
        """
        :return: ip address of the given cloudSQL instance
        """
        try:
            response = self.sqladmin().instances().get(project=project, instance=instance).execute()
            for address in response.get("ipAddresses"):
                if address.get("type") == "PRIMARY":
                    return address.get("ipAddress")
            return None
        except Exception as error:
            self._logger.warning("Could not GET gcp_host for cloudsql instance: {} {}".format(instance, error))

    def list_projects(self):
        if self._projects_cache is None:
            result = self.resource_api().projects().list().execute().get("projects")
            self._projects_cache = {project.get("name"): project for project in result}
            self._logger.debug(f"discovered project names: {str(list(self._projects_cache.keys()))}")
        return self._projects_cache

    def _await_operation(self, get_op):
        timeout = 120
        start_time = time.time()

        def is_done(op):
            if 'done' in op:
                return op['done']
            if 'status' in op:
                return op['status'] == 'DONE'
            raise Exception(f"unable to get status of op: {list(op.keys())}")

        operation = {'status': 'x'}
        while is_done(operation) and time.time() - start_time < timeout:
            operation = get_op()
            time.sleep(1)
        if is_done(operation):
            raise Exception(f"operation {operation['name']} did not complete: {operation['status']}")
