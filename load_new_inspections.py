#!/usr/bin/python3

"""
Load new OSHA inspection records from a csv file.
"""

import argparse
import csv
from datetime import datetime
import logging

import cx_Oracle

STMT_TEXT = """INSERT INTO osha_inspections_new
 (activity_nbr, reporting_id, state_flag,
  business_name, site_street_addr, site_city, site_state, site_zip_code,
  ownership_type_cd, owner_cd,
  advance_notice, safety_or_health_cd, sic, naics,
  inspection_type, inspection_scope_cd, why_no_inspection, union_cd,
  safety_manufacturing, safety_construction, safety_maritime,
  health_manufacturing, health_construction, health_maritime, migrant,
  emp_address, emp_city, emp_state, emp_zip5,
  host_est_key, nbr_in_establishment,
  open_date, case_modified_date, closing_conference_date, close_case_date,
  loaded_date)
VALUES
 (:activity_nr, :reporting_id, :state_flag,
  :estab_name, :site_address, :site_city, :site_state, :site_zip,
  :owner_type, :owner_code,
  :adv_notice, :safety_hlth, :sic_code, :naics_code,
  :insp_type, :insp_scope, :why_no_insp, :union_status,
  :safety_manuf, :safety_const, :safety_marit,
  :health_manuf, :health_const, :health_marit, :migrant,
  :mail_street, :mail_city, :mail_state, :mail_zip,
  :host_est_key, :nr_in_estab,
  :open_date, :case_mod_date, :close_conf_date, :close_case_date,
  :ld_dt)"""

FORMATS_BY_LENGTH = {10 : '%Y-%m-%d',
                     19: '%Y-%m-%d %H:%M:%S',
                     23: '%Y-%m-%d %H:%M:%S %Z'}

DEBUGGING = False

def load_new_inspections(pathname_in, pathname_bad):
    """ Straight-up insert into OSHA_INSPECTIONS_NEW of rows
    from a csv. Any records that cannot be written to the database will
    be written to the csv at pathname_bad"""

    def get_connection():
        """ We are connecting to UNICORE@pdb5, and setting autocommit on. """

        passwd = input('password for unicore: ')
        retval = cx_Oracle.connect('unicore', passwd, 'pdb5')
        retval.autocommit = not DEBUGGING

        return retval

    def apply_dates(row):
        """ I can't get the TZD format specifier to work, so
        let's convert all date strings to dates. """

        for name in ['open_date', 'case_mod_date',
                     'close_conf_date', 'close_case_date', 'ld_dt']:
            if row[name] == '':
                row[name] = None
            else:
                row[name] = datetime.strptime(row[name],
                                              FORMATS_BY_LENGTH[len(row[name])])

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
    logging.info(f'attempted {attempted} inserts') 

parser = argparse.ArgumentParser('a script to load up new inspections')
parser.add_argument('pathname_in',
                    help='pathname of a CSV containing inspection data')
parser.add_argument('pathname_bad',
                    help='pathname of a CSV for unloadable records')
args = parser.parse_args()
load_new_inspections(args.pathname_in, args.pathname_bad)
