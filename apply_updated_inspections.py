#!/usr/bin/python3

"""
Apply updated OSHA inspection records from a CSV file.
"""

import argparse
import csv
from datetime import datetime
import logging

import cx_Oracle

STMT_TEXT = """UPDATE osha_inspections_new
SET reporting_id = : reporting_id,
  state_flag = : state_flag,
  business_name = : estab_name,
  site_street_addr = : site_address,
  site_city = : site_city,
  site_state = : site_state,
  site_zip_code = : site_zip,
  ownership_type_cd = : owner_type,
  owner_cd = : owner_code,
  advance_notice = : adv_notice,
  safety_or_health_cd = : safety_hlth,
  sic = : sic_code,
  naics = : naics_code,
  inspection_type = : insp_type,
  inspection_scope_cd = : insp_scope,
  why_no_inspection = : why_no_insp,
  union_cd = : union_status,
  safety_manufacturing = : safety_manuf,
  safety_construction = : safety_const,
  safety_maritime = : safety_marit,
  health_manufacturing = : health_manuf,
  health_construction = : health_const,
  health_maritime = : health_marit,
  migrant = : migrant,
  emp_address = : mail_street,
  emp_city = : mail_city,
  emp_state = : mail_state,
  emp_zip5 = : mail_zip,
  host_est_key = : host_est_key,
  nbr_in_establishment = : nr_in_estab,
  open_date = : open_date,
  case_modified_date = : case_mod_date,
  closing_conference_date = : close_conf_date,
  close_case_date = : close_case_date,
  loaded_date = : ld_dt
WHERE activity_nbr = :activity_nr"""

FORMATS_BY_LENGTH = {10: '%Y-%m-%d',
                     19: '%Y-%m-%d %H:%M:%S',
                     23: '%Y-%m-%d %H:%M:%S %Z'}

DEBUGGING = False


def apply_updated_inspections(pathname_in, pathname_bad):
    """ Update OSHA_INSPECTIONS_NEW with rows from a csv.
    Any records that cannot be written to the database will be written to
    the csv at pathname_bad"""

    def get_connection():
        """ We are connecting to UNICORE@pdb5, and setting autocommit
        appropriately. """

        passwd = input('password for unicore: ')
        retval = cx_Oracle.connect('unicore', passwd, 'pdb5')
        retval.autocommit = not DEBUGGING

        return retval

    def apply_dates(row):
        """ OSHA is inconsistent in its date formats. We can't just wrap a
        bind position with a To_Date function, so we convert strings to
        datetimes here. """

        for name in ['open_date', 'case_mod_date',
                     'close_conf_date', 'close_case_date', 'ld_dt']:
            if row[name] == '':
                row[name] = None
            else:
                fmt = FORMATS_BY_LENGTH[len(row[name])]
                row[name] = datetime.strptime(row[name], fmt)

        return row

    def make_dbwrite(cursor, writer):
        """ avoid cluttering the main procedure with error handling,
        mostly """

        def _inner(data):
            """ the actual insert and error recording. """

            cursor.executemany(STMT_TEXT, data, batcherrors=True)
            for error in cursor.getbatcherrors():
                logging.error(error.message + ' on activity nbr '
                              + data[error.offset]['activity_nr'])
                writer.writerow(data[error.offset])

        return _inner

    logging.basicConfig(level=logging.INFO)
    conn = get_connection()
    cursor = conn.cursor()
    data = []
    attempted = 0
    with open(pathname_in, 'r') as ifh:
        reader = csv.DictReader(ifh)
        with open(pathname_bad, 'w') as ofh:
            writer = csv.DictWriter(ofh, reader.fieldnames,
                                    lineterminator='\n')
            writer.writeheader()
            dbwrite = make_dbwrite(cursor, writer)
            for row in reader:
                data.append(apply_dates(row))
                if len(data) == 1000:
                    dbwrite(data)
                    attempted += len(data)
                    data = []
                    if DEBUGGING and attempted >= 1000:
                        break
            if len(data):
                dbwrite(data)
                attempted += len(data)
    logging.info(f'attempted {attempted} updates')


parser = argparse.ArgumentParser('a script to apply updated inspections')
parser.add_argument('pathname_in',
                    help='pathname of a CSV containing inspection data')
parser.add_argument('pathname_bad',
                    help='pathname of a CSV for unloadable records')
args = parser.parse_args()
apply_updated_inspections(args.pathname_in, args.pathname_bad)
