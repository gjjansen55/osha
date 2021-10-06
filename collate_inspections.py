#!/usr/bin/python3

"""
Given the inspections that we already have in our database, locate those in the
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


def collate_inspections(csv_directory, pathname_new, pathname_updated):
    """ First, build a dictionary of the inspections that we have. The
    key is activity_number  the value is LOADED_DATE.

    That done, we will read through the CSVs, inspecting activity_nr,
    and ld_dt. We will use the first as a key for the lookup into our
    dictionary of known violations. If there is no entry, we will write the
    record out to pathname_new. If there is, we will convert the load_date
    string into a datetime, and compare it to what we have in the existing
    record. If it is newer, we will write the row out to pathname_updated. """

    def build_inspection_dictionary():
        """ Open a cursor on OSHA_INSPECTIONS_NEW, build and return the
        dictionary mentioned above.

        The ORDER BY clause in the query was there for debugging purposes,
        but does no particular harm.
        """

        retval = {}
        retrieved = 0
        conn = afl.dbconnections.connect('unicore_helper')
        cur = conn.cursor()
        cur.execute("""SELECT activity_nbr, loaded_date
        FROM unicore.osha_inspections_new
        ORDER BY activity_nbr""")
        for activity_nbr, loaded_date in cur:
            retrieved += 1
            retval[f'{activity_nbr}'] = loaded_date
            if DEBUGGING and retrieved > 500:
                break

        return retval

    def make_writer(ofh, fieldnames):

        retval = csv.DictWriter(ofh, fieldnames=fieldnames,
                                lineterminator='\n')
        retval.writeheader()

        return retval

    logging.basicConfig(level=logging.INFO)
    current_inspections = build_inspection_dictionary()
    logging.info('current inspections collected')
    new_writer, upd_writer = None, None
    csv_pathnames = glob.glob(os.path.join(csv_directory,
                                           'osha_inspection*.csv'))
    new, updated, inspected = 0, 0, 0
    with open(pathname_new, 'w') as ofh_new:
        with open(pathname_updated, 'w') as ofh_upd:
            for pathname in csv_pathnames:
                with open(pathname, 'r') as ifh:
                    reader = csv.DictReader(ifh)
                    if new_writer is None:
                        new_writer = make_writer(ofh_new,
                                                 reader.fieldnames)
                    if upd_writer is None:
                        upd_writer = make_writer(ofh_upd,
                                                 fieldnames=reader.fieldnames)
                    for row in reader:
                        key = row['activity_nr']
                        if DEBUGGING and inspected < 5:
                            logging.info(f'key in file is {key}')
                        if key not in current_inspections:
                            new_writer.writerow(row)
                            new += 1
                        else:
                            load_date = datetime.strptime(
                                    row['ld_dt'], OSHA_DATE_FORMAT)
                            if load_date > current_inspections[key]:
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
    """Extract from the CSVs such inspections as we do not have,
or which we do not have in their newest form.""")
parser.add_argument('csv_directory',
                    help='directory where the osha_inspection*.csv files are')
parser.add_argument('pathname_new',
                    help='pathname of the CSV for new records')
parser.add_argument('pathname_updated',
                    help='pathname of the CSV for updated records')
args = parser.parse_args()
collate_inspections(args.csv_directory,
                    args.pathname_new, args.pathname_updated)
