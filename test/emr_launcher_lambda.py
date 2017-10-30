#Import classes from aws package
from aws import Connection
from aws import EMRInstance
from aws import S3Manager
from manifest import ManifestParser
import os
import urllib
import boto3
import time
import sys
import logging

# Set log level
logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_manifest_file(event, manifest_file_path, manifest_file):
    """
    This function downloads the manifest file that triggers this lambda function
    :param event: Event object that contains the bucket name and key of the manifest file that triggered the lambda
    :param manifest_file_path: The local directory where the manifest file will be downloaded (usually /tmp for lambda)
    :param manifest_file: The name of the local copy of the file (usually manifest.json for lambda)
    :return: Does not return anythig, the file is simply downloaded to {manifest_file_path}/{manifest file}
    """
    bucket = event['Records'][0]['s3']['bucket']['name']
    logger.debug("Bucket: {}".format(bucket))
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))

    # Download the manifest file to /tmp - doing this as a temp measure before I
    # figure out how to extract the contents of the file from event
    try:
        s3_client = boto3.client('s3')
        s3_client.download_file(bucket, key, '{}/{}'.format(manifest_file_path, manifest_file))
    except:
        raise


def lambda_handler(event, context):
    """
    This is the main entry point for lambda function
    :param event: Event object that contains the bucket name and key of the manifest file that triggered the lambda
    :param context: Contains context information about the lambda function itself e.g. remaining time
    :return: Does not return anything
    """
    # set environment
    exec_environment = os.environ['exec_environment']
    etl_file_path = os.environ['etl_file_path']
    manifest_file_path = os.environ['manifest_file_path']
    manifest_file = os.environ['manifest_file']
    log_uri = os.environ['log_uri']

    # Download manifest file
    get_manifest_file(event, manifest_file_path, manifest_file)


    # Instantiate Connection
    conn = Connection()
    # Create an EMR connection
    conn_emr = conn.emr_connection()
    # Create an S3 connection
    conn_s3 = conn.s3_connection()
    # Instantiate S3Manager
    s3_manager = S3Manager()


    # Parse the manifest file and generate etl from ETL template wth placeholder values filled in.
    # Also gets the details about the EMR cluster to create.
    try:
        # Instantiate ManifestParser
        manifest_parser = ManifestParser()
        logger.info("Generating new ETL file from ETL template wth placeholder values filled in")
        dest_etl_file = manifest_parser.parse_manifest_file(manifest_file_path, manifest_file, conn_s3, s3_manager,
                                                            exec_environment)
        logger.info("Generated: {}".format(dest_etl_file))
    except:
        logger.error(
            'Failed while trying to generate a new ETL from s3://{}/{}'.format(manifest_parser.script_s3_bucket,
                                                                               manifest_parser.script_s3_key))
        logging.error(sys.exc_info())
        raise

    # Copy generated ETL to S3. This will then be submitted to EMR
    try:
        s3_manager.upload_object(conn_s3, etl_file_path, dest_etl_file, manifest_parser.script_s3_bucket,
                                 'generated-etls',
                                 dest_etl_file)
    except:
        raise


    # Launch and submit jobs to EMR
    try:
        # Instantiate EMR Instance
        emr = EMRInstance()

        cluster_name = "{}_{}".format(exec_environment, manifest_parser.script_s3_key)
        cluster_id = emr.get_first_available_cluster(conn_emr)

        if manifest_parser.use_existing_cluster and cluster_id:
            instance_groups = emr.get_instance_groups(conn_emr, cluster_id)
            group_id = instance_groups['CORE']

            instance_groups_count = emr.get_instance_groups_count(conn_emr, cluster_id)
            current_instance_count = instance_groups_count[group_id]

            if manifest_parser.instance_count > current_instance_count:
                emr.set_instance_count(conn_emr, cluster_id, group_id, manifest_parser.instance_count)
                # Allow 10 secs for resizing to start
                time.sleep(10)

            #submit job
            emr.submit_job(conn_emr, cluster_id, 's3://{}/generated-etls/{}'.format(manifest_parser.script_s3_bucket,
                                                                         dest_etl_file), dest_etl_file, 'cluster', 'CONTINUE')
        else:
            # Launch EMR cluster
            emr.launch_emr_and_submit_job(conn_emr, log_uri,
                                          's3://{}/generated-etls/{}'.format(manifest_parser.script_s3_bucket,
                                                                             dest_etl_file),
                                          dest_etl_file, 'cluster', 'CONTINUE', '{}'.format(cluster_name),
                                          manifest_parser.terminate_cluster, manifest_parser.instance_type,
                                          manifest_parser.instance_count)

        logger.info("Submitted s3://{}/{} to process_{}".format(manifest_parser.script, dest_etl_file, cluster_name))
    except:
        logger.error("Failed while trying to launch EMR cluster. Details below:")
        raise

'''

if __name__ == "__main__":
    # set environment
    exec_environment = 'nonprod'
    etl_file_path = '/tmp'
    #s3_bucket_etl_templates = os.environ['s3_bucket_etl_templates']
    #s3_bucket_destination = os.environ['s3_bucket_destination']
    #etl_template = os.environ['etl_template']
    manifest_file_path = '/tmp/'
    manifest_file = 'manifest_dcm_api_pg.json'
    log_uri = 's3://aws-logs-933886674506-eu-west-1/elasticmapreduce/'

    # Download manifest file
   # get_manifest_file(event, manifest_file_path, manifest_file)

    # Instantiate Connection
    conn = Connection()

    # Create an EMR connection
    conn_emr = conn.emr_connection()

    # Instantiate EMR Instance
    emr = EMRInstance()

    # Create an S3 connection
    conn_s3 = conn.s3_connection()

    # Instantiate S3Manager
    s3_manager = S3Manager()

    # Instantiate ManifestParser
    manifest_parser = ManifestParser()

    # Generate etl from ETL template wth placeholder values filled in
    try:
        logger.info("Generating new ETL file from ETL template wth placeholder values filled in")
        dest_etl_file = manifest_parser.parse_manifest_file(manifest_file_path, manifest_file, conn_s3, s3_manager,
                                                            exec_environment)
        logger.info("Generated: {}".format(dest_etl_file))
    except:
        logger.error(
            'Failed while trying to generate a new ETL from s3://{}/{}'.format(manifest_parser.script_s3_bucket,
                                                                               manifest_parser.script_s3_key))
        logging.error(sys.exc_info())
        raise

    # Copy generated ETL to S3. This will then be submitted to EMR
    try:
        s3_manager.upload_object(conn_s3, etl_file_path, dest_etl_file, manifest_parser.script_s3_bucket,
                                 'generated-etls',
                                 dest_etl_file)
    except:
        raise

    try:
        cluster_name = "{}_{}".format(exec_environment, manifest_parser.script_s3_key)
        cluster_id = emr.get_first_available_cluster(conn_emr)

        if manifest_parser.use_existing_cluster and cluster_id:
            instance_groups = emr.get_instance_groups(conn_emr, cluster_id)
            group_id = instance_groups['CORE']

            instance_groups_count = emr.get_instance_groups_count(conn_emr, cluster_id)
            current_instance_count = instance_groups_count[group_id]

            if manifest_parser.instance_count > current_instance_count:
                emr.set_instance_count(conn_emr, cluster_id, group_id, manifest_parser.instance_count)
                # Allow 10 secs for resizing to start
                time.sleep(10)

            #submit job
            emr.submit_job(conn_emr, cluster_id, 's3://{}/generated-etls/{}'.format(manifest_parser.script_s3_bucket,
                                                                         dest_etl_file), dest_etl_file, 'cluster', 'CONTINUE')
        else:
            # Launch EMR cluster
            emr.launch_emr_and_submit_job(conn_emr, log_uri,
                                          's3://{}/generated-etls/{}'.format(manifest_parser.script_s3_bucket,
                                                                             dest_etl_file),
                                          dest_etl_file, 'cluster', 'CONTINUE', '{}'.format(cluster_name),
                                          manifest_parser.terminate_cluster, manifest_parser.instance_type,
                                          manifest_parser.instance_count)

        logger.info("Submitted s3://{}/{} to process_{}".format(manifest_parser.script, dest_etl_file, cluster_name))
    except:
        logger.error("Failed while trying to launch EMR cluster. Details below:")
        raise
'''