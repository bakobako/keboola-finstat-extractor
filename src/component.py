'''
Finstat Extractor for Keboola

Takes an input csv file containing company ICO and outputs
a csv containing the company data from Finstat

Input csv file should have a "ico" column,
if not it will take the first column of the csv file
as the ico column

Author: Adam Bako
'''


import logging
import os
import sys
import requests
import hashlib
import xmltodict
import pandas as pd
from datetime import datetime
from pathlib import Path

from kbc.env_handler import KBCEnvHandler

# configuration variables
URL = "https://finstat.sk/api/detail"

# #### Keep for debug
KEY_DEBUG = 'debug'

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
MANDATORY_PARS = ['#api_key','#private_key']
MANDATORY_IMAGE_PARS = []

APP_VERSION = '0.1.0'

def encrypt_string(hash_string):
    """Encrypts a string with sha256

            Parameters:
            hash_string (string): Holds the string to be hashed

            Returns:
            sha_signature (string): Holds the hashed string
    """
    sha_signature = \
        hashlib.sha256(hash_string.encode()).hexdigest()
    return sha_signature

def get_hash(api_key,private_key,ico):
    """Creates a hash key that is needed for the api

        The API defines how this key should be constructed in the API documentation

            Parameters:
            api_key (string): The API key of the API user
            private_key (string): The private key of the API user
            ico (string): The ICO code of the

            Returns:
            hash_key (string): Holds the hashed key
    """
    hash_key_string = "SomeSalt+" + api_key + "+" + private_key + "++" + ico + "+ended"
    hash_key = encrypt_string(hash_key_string)
    return hash_key


def flatten_json(json_dict, delim):
    """Flattens a JSON dictionary so it can be stored in a single table row

            Parameters:
            json_dict (dict): Holds the json data
            delim (string): The delimiter to be used to create flattened keys

            Returns:
            flattened_dict (dict): Holds the flattened dictionary
    """
    flattened_dict = {}
    for i in json_dict.keys():
        if isinstance(json_dict[i], dict):
            get = flatten_json(json_dict[i], delim)
            for j in get.keys():
                flattened_dict[i + delim + j] = get[j]
        else:
            flattened_dict[i] = json_dict[i]

    return flattened_dict

def get_json_response(params):
    """Uses the API to get a single response

        The XML response of the API is converted to JSON.
        If ICO is invalid it returns False

            Parameters:
            params (dict): Holds the parameters of the API call

            Returns:
            json_response (dict): Holds the JSON response
    """
    # sending get request and saving the response as response object
    response = requests.get(url=URL, params=params)

    if response.status_code == 200:
        # If successful return the result
        json_response = dict(xmltodict.parse(response.text)["DetailResult"])
        return json_response
    else:
        print(f"Error : ico {params['ico']} is not a valid ico in the Finstat database")
        return False


def get_icos_from_file(filepath):
    """Retrieves ICOs to be fetched from a CSV file

        First looks for an ico column, if this does not exist,
        it takes the first column in the csv file

            Parameters:
            filepath (string): Holds the path to the csv file

            Returns:
            icos (list): Holds a list of ICO form the file
    """
    ico_df = pd.read_csv(filepath)
    cols = ico_df.columns
    if "ico" in cols :
        icos = list(ico_df["ico"])
    else:
        icos = list(ico_df[cols[0]])
    return icos


class Component(KBCEnvHandler):

    def __init__(self, debug=False):
        # for easier local project setup
        default_data_dir = Path(__file__).resolve().parent.parent.joinpath('data').as_posix() \
            if not os.environ.get('KBC_DATADIR') else None
        KBCEnvHandler.__init__(self, MANDATORY_PARS,
                               log_level=logging.DEBUG if debug else logging.INFO,
                               data_path=default_data_dir)
        # override debug from config
        if self.cfg_params.get(KEY_DEBUG):
            debug = True
        if debug:
            logging.getLogger().setLevel(logging.DEBUG)
        logging.info('Running version %s', APP_VERSION)
        logging.info('Loading configuration...')

        try:
            # validation of mandatory parameters. Produces ValueError
            self.validate_config(MANDATORY_PARS)
            self.validate_image_parameters(MANDATORY_IMAGE_PARS)
        except ValueError as error:
            logging.exception(error)
            exit(1)
        # ####### EXAMPLE TO REMOVE
        #         # intialize instance parameteres
        #
        #         # ####### EXAMPLE TO REMOVE END

    def run(self):
        '''
        Main execution code
        '''
        params = self.cfg_params  # noqa

        current_datetime = str(datetime.now().now())\
            .replace(" ", "-")\
            .replace(":", "-")\
            .split(".")[0]
        filename = "finstat-out-"+current_datetime+'.csv'

        SOURCE_FILE_PATH = self.get_input_tables_definitions()[0].full_path
        RESULT_FILE_PATH = os.path.join(self.tables_out_path, filename)

        PARAM_API_KEY = params['#api_key']
        PARAM_PRIVATE_KEY = params['#private_key']

        #  make manifest file for output, set primary key and incremental load
        self.configuration.write_table_manifest(file_name=RESULT_FILE_PATH)

        print('Running...')
        with open(SOURCE_FILE_PATH, 'r') as input:
            icos = get_icos_from_file(input)
            json_responses = []

            for ico in icos:
                hash_key = get_hash(PARAM_API_KEY, PARAM_PRIVATE_KEY, str(ico))
                # defining a params dict for the parameters to be sent to the API
                PARAMS = {'ico': str(ico),
                          "apiKey": PARAM_API_KEY,
                          "Hash": hash_key}
                print(f"Getting Finstat data for ico : {ico}")
                response = get_json_response(PARAMS)
                if response:
                    json_responses.append(response)

            for i, response in enumerate(json_responses):
                json_responses[i] = flatten_json(json_responses[i], "__")

        dfItem = pd.DataFrame.from_records(json_responses)
        dfItem.to_csv(RESULT_FILE_PATH, index=False)

        print(RESULT_FILE_PATH)

        # print state file
        previous_state = self.get_state_file()
        update_date = previous_state.get("last_update", " ")
        logging.info('Previous update on: %s', update_date)

        # update state file with current date
        current_date = str(datetime.now())
        self.write_state_file({"last_update": current_date})
        logging.info('Updating state to : %s', current_date)


"""
        Main entrypoint
"""
if __name__ == "__main__":
    if len(sys.argv) > 1:
        debug_arg = sys.argv[1]
    else:
        debug_arg = False
    try:
        comp = Component(debug_arg)
        comp.run()
    except Exception as exc:
        logging.exception(exc)
        exit(1)