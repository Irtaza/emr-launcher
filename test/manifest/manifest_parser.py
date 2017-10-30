import json
from collections import OrderedDict
import time
import logging

# Set log level
logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class ManifestParser:

    def __init__(self):
        '''Manifest Parser constructor'''
        self.instance_count = 0
        self.instance_type = 'm3.xlarge'
        self.use_existing_cluster = False
        self.terminate_cluster = True


    def json_to_dict(self, src_file_path, ordered_dict=False):
        '''
        Reads a json file and converts the Json into a dictionary object
        :param src_file_path: The file that has the JSON data
        :param ordered_dict: Create Ordered Dictionary where original order of JSON has to be kept e.g. DataFlow
        :return: Returns a JSON dictionary
        '''
        json_data = open(src_file_path)
        if (ordered_dict):
            json_dict = json.load(json_data, object_pairs_hook=OrderedDict)
        else:
            json_dict = json.load(json_data)
        # json_dict = OrderedDict(json_data)
        return json_dict


    def generate_etl_from_template(self, src_file, dest_file, list_of_replacements):
        '''
        Generates a new file from a template by replacing all placeholders in the template with values given in the
        'replacements' list
        :param src_file: The template file that has the placeholders
        :param dest_file: The generated file that replaces placeholders with actual values listed in 'replacements'
        :param list_of_replacements: A list of dictionary items with placeholders and their corresponding values
        :return:N/A
        '''
        with open(src_file) as infile, open(dest_file, 'w') as outfile:
            for line in infile:
                for replacements in list_of_replacements:
                    for src, target in list(replacements.items()):
                        line = line.replace(src, target)
                outfile.write(line)


    def parse_bool_string(self, string_to_parse):
        '''
        Converts string "True" and "False" values to boolean True and False
        :param string_to_parse:The string that contains either "True" or "False"
        :return: returns a boolean value True  or False
        '''
        return string_to_parse[0].upper() == 'T'


    def get_etl_details(self, dict):
        '''
        Gets values for ETL related values from manifest file and populates class attributes
        :param dict: The manifest dictionary
        :return: N/A
        '''
        self.script = dict['etl']['script']
        self.script_type = dict['etl']['type']
        self.script_s3_bucket = dict['etl']['script_s3_bucket']
        self.script_s3_key = dict['etl']['script_s3_key']


    def get_resource_details(self, dict):
        '''
        Gets values for EMR related values from manifest file and populates class attributes
        :param dict: The manifest dictionary
        :return: N/A
        '''
        self.instance_type = dict['resource']['instance_type']
        self.instance_count = int(dict['resource']['instance_count'])
        self.use_existing_cluster = self.parse_bool_string(dict['resource']['use_existing_cluster'])
        self.terminate_cluster =  self.parse_bool_string(dict['resource']['terminate_cluster'])


    def get_replacements(self, dict):
        '''
        Gets values for placeholders from manifest file
        :param dict: The manifest dictionary
        :return: A list of dictionary items that contains placeholders and their corresponding values
        '''
        replacements = []
        source_dict = dict['source']
        replacements.append(source_dict)

        placeholder_dict = dict['placeholder']
        replacements.append(placeholder_dict)

        return replacements


    def get_etl(self, s3_bucket, src_etl_name, conn_s3, s3_manager, replacements, exec_environment):
        '''
        Downloads the ETL template file from S3 and then calls 'generate_etl_from_template' function to
        create a new version of the ETL file with placeholder values replaced with values defined in the
        manifest file
        :param s3_bucket: The bucket that contains the template ETL
        :param src_etl_name: The name of the ETL template
        :param conn_s3: An instance of S3 connection object from Connection class
        :param s3_manager: An instance of S3Manger class
        :param replacements: A list of dictionary items with placeholders and their corresponding values
        :param exec_environment: Execution environment e.g. nonprod
        :return: Name of the generated ETL file from the template
        '''
        # Download ETL template
        src_etl_file = s3_manager.download_object(conn_s3, s3_bucket, src_etl_name)
        logger.info("Source ETL File: {}".format(src_etl_file))

        # Generate a new temp ETL file
        exec_time = str(int(time.time()))
        dest_etl_file = src_etl_file.replace('.py', '') + '_' + exec_environment + '_' + exec_time + '.py'
        self.generate_etl_from_template(src_etl_file, dest_etl_file, replacements)

        return dest_etl_file.replace('/tmp/', '')

    def parse_manifest_file(self, src_manifest_path, src_manifest_file, conn_s3, s3_manager, exec_environment):
        '''
        Parses the manifest file; populates class attributes; generates new ETL file
        :param src_manifest_path: The local directory where the manifest file is downloaded (usually /tmp for lambda)
        :param src_manifest_file: The name of the local copy of the file (usually manifest.json for lambda)
        :param conn_s3: An instance of S3 connection object from Connection class
        :param s3_manager: An instance of S3Manger class
        :param exec_environment: Execution environment e.g. nonprod
        :return: Name of the generated ETL file from the template
        '''
        manifest_dict = self.json_to_dict('{}/{}'.format(src_manifest_path, src_manifest_file))
        print(manifest_dict)

        # Get instance type and number of instances from the manifest file
        self.get_etl_details(manifest_dict)
        self.get_resource_details(manifest_dict)
        replacements = self.get_replacements(manifest_dict)

        # Download the ETL file from S3 & generate a new temp ETL File
        dest_etl_file = self.get_etl(self.script_s3_bucket, self.script_s3_key, conn_s3, s3_manager, replacements, exec_environment)

        return dest_etl_file
