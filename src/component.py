'''
Template Component main class.

'''

import logging
import os
import sys
import csv
import requests
import hashlib
import xmltodict
import pandas as pd
from datetime import datetime
from pathlib import Path

from kbc.env_handler import KBCEnvHandler

# configuration variables
KEY_API_TOKEN = '#api_token'
KEY_PRINT_HELLO = 'print_hello'
URL = "https://finstat.sk/api/detail"

# #### Keep for debug
KEY_DEBUG = 'debug'

# list of mandatory parameters => if some is missing, component will fail with readable message on initialization.
MANDATORY_PARS = ['api_key','private_key',KEY_DEBUG]
MANDATORY_IMAGE_PARS = []

APP_VERSION = '0.0.1'

def encrypt_string(hash_string):
    sha_signature = \
        hashlib.sha256(hash_string.encode()).hexdigest()
    return sha_signature

def get_hash(api_key,private_key,ico):
    hash_key_string = "SomeSalt+" + api_key + "+" + private_key + "++" + ico + "+ended"
    hash_key = encrypt_string(hash_key_string)
    return hash_key


def flatten_json(b, delim):
    val = {}
    for i in b.keys():
        if isinstance(b[i], dict):
            get = flatten_json(b[i], delim)
            for j in get.keys():
                val[i + delim + j] = get[j]
        else:
            val[i] = b[i]

    return val

def get_json_response(PARAMS):
    # sending get request and saving the response as response object
    r = requests.get(url=URL, params=PARAMS)
    if r.status_code == 200:
        json_response = dict(xmltodict.parse(r.text)["DetailResult"])
        return json_response
    else:
        print(f"Error : ico {PARAMS['ico']} is not a valid ico in the Finstat database")
        return False


def save_to_csv(json_responses,columns):
    with open("json_out.csv", 'w') as csv_file:
        writer = csv.writer(csv_file, delimiter=',')
        writer.writerow(columns)

        for i_r in json_responses:
            writer.writerow(map(lambda x: i_r.get(x, ""), columns))


def get_icos_from_file(filename):
    ico_df = pd.read_csv(filename)
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

        KBCEnvHandler.__init__(self, MANDATORY_PARS, log_level=logging.DEBUG if debug else logging.INFO,
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
        except ValueError as e:
            logging.exception(e)
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

        SOURCE_FILE_PATH = self.get_input_tables_definitions()[0].full_path
        RESULT_FILE_PATH = os.path.join(self.tables_out_path, 'out.csv')

        PARAM_API_KEY = params['api_key']
        PARAM_PRIVATE_KEY = params['private_key']

        #  make manifest file for output, set primary key and incremental load
        self.configuration.write_table_manifest(file_name=RESULT_FILE_PATH)

        print('Running...')
        with open(SOURCE_FILE_PATH, 'r') as input, open(RESULT_FILE_PATH, 'w+', newline='') as out:
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
                    print(1)
                    json_responses.append(response)

            for i, response in enumerate(json_responses):
                json_responses[i] = flatten_json(json_responses[i], "__")

            columns = [x for row in json_responses for x in row.keys()]
            columns = list(set(columns))
            writer = csv.DictWriter(out, fieldnames=columns, lineterminator='\n', delimiter=',')
            writer.writeheader()
            for index, l in enumerate(json_responses):
                writer.writerow(map(lambda x: l.get(x, ""), columns))

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
