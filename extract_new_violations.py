#!/usr/bin/python3

"""
Given the violations that we already have in our database, locate those in the
csvs provided that are not present in the database, or which were loaded more
recently.
"""

import argparse
import csv
from datetime import datetime
import glob
import logging
import os.path

import afl.dbconnections

DEBUGGING = False
OSHA_DATE_FORMAT = '%Y-%m-%d %H:%M:%S %Z'


def extract_new_violations(csv_directory, pathname_new, pathname_updated):
    """ First, build a dictionary of the violations that we have. The
    key will be activity_number:citation_id, the value is LOAD_DATE.

    That done, we will read through the CSVs, inspecting activity_nr,
    citation_id, and load_dt. We will concatenate the first two to create
    a key for the lookup into our dictionary of known violations. If
    there is no entry, we write the record out to pathname_new. If there is,
    we convert the load_date string into a datetime, and compare it to what we
    have in the existing record. If it is newer, we write the value out.  """

    def build_violation_dictionary():
        """ open a cursor on OSHA_VIOLATIONS_NEW, build the dictionary
        as mentioned above. """

        retval = {}
        retrieved = 0
        conn = afl.dbconnections.connect('unicore_helper')
        cur = conn.cursor()
        cur.execute("""SELECT activity_nbr, citation_id, loaded_date
        FROM unicore.osha_violations_new
        ORDER BY activity_nbr, citation_id""")
        for activity_nbr, citation_id, loaded_date in cur:
            key = f'{activity_nbr}:{citation_id}'
            if DEBUGGING and retrieved < 5:
                logging.info(f'key from db is {key}')
            retrieved += 1
            retval[key] = loaded_date
            if DEBUGGING and retrieved > 500:
                break

        return retval

    def make_writer(ofh, fieldnames):

        retval = csv.DictWriter(ofh, fieldnames=fieldnames,
                                lineterminator='\n')
        retval.writeheader()

        return retval

    logging.basicConfig(level=logging.INFO)
    current_violations = build_violation_dictionary()
    logging.info('current violations collected')
    new_writer, upd_writer = None, None
    csv_pathnames = glob.glob(os.path.join(csv_directory,
                                           'osha_violation*.csv'))
    new, updated, inspected = 0, 0, 0
    with open(pathname_new, 'w') as ofh_new:
        with open(pathname_updated, 'w') as ofh_upd:
            for pathname in csv_pathnames:
                with open(pathname, 'r') as ifh:
                    reader = csv.DictReader(ifh)
                    if new_writer is None:
                        new_writer = make_writer(ofh_new, reader.fieldnames)
                    if upd_writer is None:
                        upd_writer = make_writer(ofh_upd, reader.fieldnames)
                    for row in reader:
                        key = f"{row['activity_nr']}:{row['citation_id']}"
                        if DEBUGGING and inspected < 5:
                            logging.info(f'key in file is {key}')
                        if key not in current_violations:
                            new_writer.writerow(row)
                            new += 1
                        else:
                            load_date = datetime.strptime(
                                    row['load_dt'], OSHA_DATE_FORMAT)
                            if load_date > current_violations[key]:
                                upd_writer.writerow(row)
                                updated += 1
                        inspected += 1
                        if DEBUGGING and inspected > 500:
                            return
                    if pathname != csv_pathnames[-1]:
                        logging.info(
                            f'done with {pathname}, {new} new records, '
                            + f'{updated} updated records written')
    print(f'wrote out {new} new records, {updated} updated records.')


parser = argparse.ArgumentParser(
    """Extract from the CSVs such violations as we do not have,
or which we do not have in their newest form.""")
parser.add_argument('csv_directory',
                    help='directory where the osha_violation*.csv files are')
parser.add_argument('pathname_new',
                    help='pathname of the CSV for new records')
parser.add_argument('pathname_updated',
                    help='pathname of the CSV for updated records')
args = parser.parse_args()
extract_new_violations(args.csv_directory,
                       args.pathname_new, args.pathname_updated)
