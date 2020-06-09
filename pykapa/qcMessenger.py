from pykapa.quality_functions import surveyCTO_download, qc_fields, qc_messenger, save_corrections
from pykapa.controllers.slack import slack_post
from pykapa.models import User
from pykapa.controllers.google_sheets import open_google_sheet, dct_xls_data
from pykapa.settings.logging import logger
from pykapa.settings.config import config_paths, setup_config_for_project, Config
from pykapa.gen_funcs import read_json_file, append_to_csv
from pykapa.controllers.slack import PykapaSlackClient
from pykapa.drop_box import to_dropbox
from pykapa.xls_functions import date_time, now
import pandas as pd
import time


def run_quality_control():
    # --------------------------------------------------------------------------------------------------------------------
    #    1.                                              User Inputs
    # --------------------------------------------------------------------------------------------------------------------
    config: Config = setup_config_for_project()
    slack_client = PykapaSlackClient(config.slack_token, config.slack_error_channel)

    # --------------------------------------------------------------------------------------------------------------------
    #    2.                                              Quality Control
    # --------------------------------------------------------------------------------------------------------------------


    def find_latest_survey_recorded(form_id):
        # find the last save for current data task (currently saved in json file)
        date_old = None
        # read json tracker
        dir_x = './data/projects/%s/qctrack.json' % form_id
        qc_track = read_json_file(dir_x)

        # filter data by CompletionDate
        if qc_track['CompletionDate'] != '':
            # date from the JSON file that stores the last record
            date_old = date_time(qc_track['CompletionDate'])
        return date_old


    def get_csv_path_for_form(form_id):
        csv_filename = '{}.csv'.format(form_id)
        csv_path = '{}/{}/{}'.format(config_paths.get("projects_folder"), form_id, csv_filename)
        return csv_path


    logger.info('Pykapa has started tracking your data and will send alerts to your specified messenger app.')
    while True:
        # process google sheet and return dictionary
        import pdb
        pdb.set_trace()
        dct_xls = dct_xls_data(config, slack_client)

        if "error" in list(dct_xls):
            err_msg = dct_xls['message']

            if "error" in err_msg:
                dct_xls = None
            else:
                break

        else:

            try:
                if dct_xls is not None:

                    form_id = dct_xls['form_id']
                    df_msg = dct_xls['messages']
                    csv_path = get_csv_path_for_form(form_id)

                    # Download data from surveyCTO
                    # retrieve data from surveyCTO as dataframe
                    df_survey = surveyCTO_download(config.scto_host, config.scto_username, config.scto_password, form_id, config.slack_error_channel)

                    if df_survey is not None:

                        if not df_survey.empty and list(df_survey) != ['error']:

                            date_old = find_latest_survey_recorded(form_id)

                            df_survey['CompletionDate'] = pd.to_datetime(df_survey['CompletionDate'])

                            df_survey = df_survey[df_survey.CompletionDate > date_old]

                            append_to_csv(csv_path, df_survey)

                            # extract columns relevant to qc messages
                            cols = qc_fields(dct_xls)
                            svy_cols = list(set(list(df_survey)).intersection(set(cols)))

                            try:
                                df_surv = df_survey[svy_cols]
                            except Exception as err:
                                slack_msg = '`Google Sheet Error:(Incorrect Variable)`\n' + str(err)
                                logger.info(slack_msg)
                                slack_post(config.slack_error_channel, slack_msg)
                                break

                            # perform quality control and post messages on slack
                            # perform quality control, post messages, and send incentives
                            qc_messenger(df_surv, dct_xls, config.slack_error_channel, config.google_sheet_url)

                    else:
                        logger.info('surveyCTO data set is unrecognized.')
                        break

                    # Perform Corrections and backup file on Dropbox
                    gsheet = open_google_sheet(config.google_sheet_url)
                    save_corrections(gsheet, csv_path, form_id)  # perform corrections and save file
                    to_dropbox(gsheet, form_id)  # upload files local datasets to dropbox

                    try:
                        df_xpt = dct_xls['export']
                        # read survey worksheet
                        # df_svy = dct_xls['select']
                    except Exception as err:
                        print('\nExport Worksheet Exception: %s' %err)

            except Exception as err:
                err_msg = '`Warning:` \n%s. \n\nYou may have to follow up on this. The backend is still running' % err
                logger.info(err_msg)
                slack_post(config.slack_error_channel, err_msg)

        logger.info('The End %s' % now())
        time.sleep(600)


if __name__ == '__main__':
    run_quality_control()
