#!/usr/bin/python3

"""
Load new OSHA violation records from a csv file.
"""

import argparse
import csv
from datetime import datetime
import logging

import cx_Oracle

STMT_TEXT = """INSERT INTO osha_violations_new
 (activity_nbr, citation_id, delete_flag,
  standard, violation_type, issuance_date, abate_date,
  abate_complete, current_penalty, initial_penalty,
  contest_date, final_order_date,
  nbr_instances, nbr_exposed, rec, gravity,
  emphasis, hazcat, fta_inspection_nbr, fta_issuance_date,
  fta_penalty, fta_contest_date, fta_final_order_date,
  hazsub1, hazsub2, hazsub3, hazsub4, hazsub5, loaded_date)
VALUES
 (:activity_nr, :citation_id, :delete_flag,
  :standard, :viol_type, :issuance_date, :abate_date,
  :abate_complete, :current_penalty, :initial_penalty,
  :contest_date, :final_order_date,
  :nr_instances, :nr_exposed, :rec, :gravity,
  :emphasis, :hazcat, :fta_insp_nr, :fta_issuance_date,
  :fta_penalty, :fta_contest_date, :fta_final_order_date,
  :hazsub1, :hazsub2, :hazsub3, :hazsub4,
  :hazsub5, :load_dt)"""

FORMATS_BY_LENGTH = {10: '%Y-%m-%d',
                     19: '%Y-%m-%d %H:%M:%S',
                     23: '%Y-%m-%d %H:%M:%S %Z'}

DEBUGGING = False


def load_new_violations(pathname_in, pathname_bad):
    """ Straight-up insert into OSHA_VIOLATIONS_NEW of rows
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

        for name in ['issuance_date', 'abate_date', 'contest_date',
                     'final_order_date', 'fta_issuance_date',
                     'fta_contest_date', 'fta_final_order_date', 'load_dt']:
            if row[name] == '':
                row[name] = None
            else:
                row[name] = datetime.strptime(
                    row[name], FORMATS_BY_LENGTH[len(row[name])])

        return row

    def make_dbwrite(cursor, writer):
        """ avoid cluttering the main procedure with error handling,
        mostly """

        def _inner(data):
            """ the actual insert and error recording. """

            cursor.executemany(STMT_TEXT, data, batcherrors=True)
            for error in cursor.getbatcherrors():
                logging.error(error.message + ' on activity nbr '
                              + data[error.offset]['activity_nr']
                              + ', ' + data[error.offset]['citation_id'])
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


parser = argparse.ArgumentParser('a script to load up violations')
parser.add_argument('pathname_in',
                    help='pathname of a CSV containing violation data')
parser.add_argument('pathname_bad',
                    help='pathname of a CSV for unloadable records')
args = parser.parse_args()
load_new_violations(args.pathname_in, args.pathname_bad)
