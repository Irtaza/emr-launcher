import time
import logging

# Set log level
logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class EMRInstance:

    def __init__(self):
        '''EMR Constructor'''

    def get_first_available_cluster(self, conn):
        '''
        Gets the cluster_id of the first available EMR cluster, if any
        :param conn: An instance of EMR connection object from Connection class
        :return: The cluster Id of the EMR cluster
        '''
        clusters = conn.list_clusters()

        # choose the correct cluster i.e. one that is either in Running or waiting state
        clusters = [c["Id"] for c in clusters["Clusters"]
            if c["Status"]["State"] in ["RUNNING", "WAITING"]]

        if not clusters:
            logger.info("No valid clusters\n")
            return None

        # take the first relevant cluster
        cluster_id = clusters[0]
        logger.info("Cluster: {} \n".format(cluster_id))

        return cluster_id

    def get_instance_groups(self, conn, cluster_id):
        '''
        Gets Instance Group Type and their Group Ids for an EMR cluster.
        :param conn: An instance of EMR connection object from Connection class
        :param cluster_id: The cluster Id of the EMR cluster
        :return: A dictionary of instance group types and their corresponding group Id e.g.{"Core": "xxxxxx"}
        '''
        instance_group_types = []
        instance_group_ids = []

        instanceGroups = conn.list_instance_groups(ClusterId=cluster_id)['InstanceGroups']

        #[(instance_group_types.append(i['InstanceGroupType']), instance_group_ids.append(i['Id'])) for i in instanceGroups]

        for i in instanceGroups:
            instance_group_types.append(i['InstanceGroupType'])
            instance_group_ids.append(i['Id'])
            logger.info(i['RunningInstanceCount'])
            logger.info(i['RequestedInstanceCount'])
            logger.info(i['Status'])

        return dict(zip(instance_group_types, instance_group_ids))


    def get_master_ec2_instance_id(self, conn, cluster_id):
        '''
        Gets the EC2 instance ID of the Master node in EMR cluster
        :param conn: An instance of EMR connection object from Connection class
        :param cluster_id: The cluster Id of the EMR cluster
        :return: The EC2 instance ID of the Master node in EMR cluster
        '''
        ec2_instance_id = conn.list_instances(ClusterId=cluster_id, InstanceGroupTypes=['MASTER'])['Instances'][0]['Ec2InstanceId']
        return ec2_instance_id


    def get_instance_groups_count(self, conn, cluster_id):
        '''
        Gets the count of instances running in each cluster group
        :param conn: An instance of EMR connection object from Connection class
        :param cluster_id: The cluster Id of the EMR cluster
        :return: A dictionary of instance group ids and the number of nodes running in each group type
        '''
        instance_group_count = []
        instance_group_ids = []

        instanceGroups = conn.list_instance_groups(ClusterId=cluster_id)['InstanceGroups']

        for i in instanceGroups:
            instance_group_ids.append(i['Id'])
            instance_group_count.append(i['RunningInstanceCount'])
            logger.info(i['RunningInstanceCount'])
            logger.info(i['RequestedInstanceCount'])
            logger.info(i['Status'])

        return dict(zip(instance_group_ids, instance_group_count))


    def set_instance_count(self, conn, cluster_id, group_id, instance_count):
        '''
        Modifies the number of nodes in an instance group.
        :param conn: An instance of EMR connection object from Connection class
        :param cluster_id: The cluster Id of the EMR cluster
        :param group_id: The Group Id of the instance that has to be modified
        :param instance_count: The new target instance count
        :return: N/A
        '''
        conn.modify_instance_groups(ClusterId=cluster_id, InstanceGroups=[{'InstanceGroupId': group_id, 'InstanceCount': instance_count}])


    def terminate_clusters(self, conn, cluster_ids):
        '''
        Shuts a list of clusters (job flows) down. When a job flow is shut down, any step not yet completed
        is canceled and the EC2 instances on which the cluster is running are stopped. Any log files not already saved
        are uploaded to Amazon S3 if a LogUri was specified when the cluster was created.
        :param conn: An instance of EMR connection object from Connection class
        :param cluster_ids: List of cluster Ids to terminate
        :return: N/A
        '''
        conn.terminate_job_flows(JobFlowIds=cluster_ids)

    def submit_job(self, conn, cluster_id, code_path, step_name, deploy_mode='cluster', action_on_failure='CONTINUE'):
        '''
        Submits a new PySpark job to an existing cluster by adding a new step to a running cluster
        :param conn: An instance of EMR connection object from Connection class
        :param cluster_id: The cluster Id of the EMR cluster
        :param code_path: The S3 URI where the PySpark code is stored
        :param step_name: The name of the step
        :param deploy_mode: "Cluster" or "Client" mode. Cluster mode launches your driver program on the cluster,
                            while client mode launches the driver program locally
        :param action_on_failure: The action to take if the step fails.
                                  CONTINUE: in the event on failure of a step, it
                                            would cancel all the remaining steps and the cluster will get into a
                                            Waiting status
                                  CANCEL_AND_WAIT: in the event on failure of a step, it would continue with the
                                                   execution of next step.

        :return: retruns a message that the job has been submitted.
        '''
        step_args = ["spark-submit", "--deploy-mode", deploy_mode, code_path]

        step = {"Name": step_name + "-" + time.strftime("%Y%m%d-%H:%M"),
                'ActionOnFailure': action_on_failure,
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': step_args
                    }
                }
        action = conn.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step])
        return "Added step: %s"%(action)


    def launch_emr_and_submit_job(self, conn, log_uri, code_path, step_name, deploy_mode='cluster',
                                  action_on_failure='CONTINUE', cluster_name = 'via_boto', terminate_cluster = False,
                                  instance_type = 'm3.xlarge', instance_count=1):
        '''
        Launches a new cluster and submits a job to that cluster by adding a step
        :param conn: An instance of EMR connection object from Connection class
        :param log_uri: The location in Amazon S3 to write the log files of the job flow.
        :param code_path: The S3 URI where the PySpark code is stored
        :param step_name: The name of the step
        :param deploy_mode: "Cluster" or "Client" mode. Cluster mode launches your driver program on the cluster,
                            while client mode launches the driver program locally
        :param action_on_failure: The action to take if the step fails.
                                  CONTINUE: in the event on failure of a step, it
                                            would cancel all the remaining steps and the cluster will get into a
                                            Waiting status
                                  CANCEL_AND_WAIT: in the event on failure of a step, it would continue with the
                                                   execution of next step.
        :param cluster_name: The name that should be given to the cluster - can be any string
        :param terminate_cluster: Specifies if the cluster is to be terminated after step is completed - boolean
        :param instance_type: The EC2 instance type to be used for Master and Slave (worker) nodes
        :param instance_count: The number of Slave EC2 instances (worker nodes) in the cluster
        :return: N/A
        '''
        keep_job_flow_alive_when_no_steps = not terminate_cluster

        logger.info("Launching EMR cluster to process {}".format(code_path))
        step_args = ["spark-submit", "--deploy-mode", deploy_mode, code_path]

        step = {"Name": step_name + "-" + time.strftime("%Y%m%d-%H:%M"),
                'ActionOnFailure': action_on_failure,
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': step_args
                }
                }

        cluster_id = conn.run_job_flow(
            Name='process_{}'.format(cluster_name),
            LogUri= log_uri,
            ReleaseLabel='emr-5.9.0',
            Instances={
                'InstanceGroups': [
                    {
                        'Name': "Master nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': instance_type,
                        'InstanceCount': 1,
                    },
                    {
                        'Name': "Slave nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': instance_type,
                        'InstanceCount': instance_count,
                    }
                ],
                'Ec2KeyName': 'dadl',
                'KeepJobFlowAliveWhenNoSteps': keep_job_flow_alive_when_no_steps,
                'TerminationProtected': False,
                'Ec2SubnetId': 'subnet-cb098f93',
            },
            Applications=[{'Name': "spark"}],
            Steps=[step],
            VisibleToAllUsers=True,
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole',
            Tags=[
                {
                    'Key': 'Processing',
                    'Value': cluster_name,
                },
            ],
        )
