""" Module to populate and interact with the COSMoS database.

.. warning ::
    The functions contained in this module require a configuration
    file to run.  This file gives the connectring string into the
    database, and is needed to run.

"""

from __future__ import print_function, absolute_import, division

from astropy.io import fits
import os
from sqlalchemy import and_, or_, text, MetaData
import sys
import matplotlib as mpl
mpl.use('Agg')
import numpy as np
import multiprocessing as mp
import types
import argparse
import pprint
import inspect
import functools
import logging
logger = logging.getLogger(__name__)

from ..cci.gainmap import make_all_hv_maps
from ..cci.gainmap import write_and_pull_gainmap
from ..cci.monitor import monitor as cci_monitor
from ..dark.monitor import monitor as dark_monitor
from ..dark.monitor import pull_orbital_info
from ..filesystem import find_all_datasets
from ..osm.monitor import pull_flashes
from ..osm.monitor import monitor as osm_monitor
from ..stim.monitor import locate_stims
from ..stim.monitor import stim_monitor
from ..utils.utils import scrape_cycle
from .db_tables import load_connection, open_settings
from .db_tables import Base
from .db_tables import Files, Lampflash, Stims, Darks, Gain, fuv_primary_headers

#-------------------------------------------------------------------------------




# BEGIN PARSRING FUNCTIONS




#-------------------------------------------------------------------------------

def db_connect(child):
    """Decorator to wrap functions that need a db connection.

    TESTING

    """

    def wrapper():
        settings = open_settings()

        Session, engine = load_connection(kwargs['settings']['connection_string'])
        session = Session()

        child(settings=settings, engine=engine, session=session)

        session.commit()
        session.close()
        engine.dispose()

    return wrapper

#-------------------------------------------------------------------------------

def call(arg):
    arg()

#-------------------------------------------------------------------------------

def mp_insert(args):
    """Wrapper function to parse arguments and pass into insertion function

    Parameters
    ----------
    args, tuple
        filename, table, function, (foreign key, optional)

    """

    if len(args) == 3:
        filename, table, function = args
        insert_with_yield(filename, table, function)
    elif len(args) == 4:
        filename, table, function, foreign_key = args
        insert_with_yield(filename, table, function, foreign_key)
    else:
        raise ValueError("Not provided the right number of args")

#-------------------------------------------------------------------------------

def insert_with_yield(filename, table, function, foreign_key=None, **kwargs):
    """ Call function on filename and insert results into table

    Parameters
    ----------
    filename : str
        name of the file to call the function argument on
    table : sqlalchemy table object
        The table of the database to update.
    function : function
        The function to call, should be a generator
    foreign_key : int, optional
        foreign key to update the table with
    """

    settings = open_settings()
    Session, engine = load_connection(settings['connection_string'])
    session = Session()

    try:
        data = function(filename, **kwargs)

        if isinstance(data, dict):
            data = [data]
        elif isinstance(data, types.GeneratorType):
            pass
        else:
            raise ValueError("Not designed to work with data of type {}".format(type(data)))

        #-- Pull data from generator and commit
        for i, row in enumerate(data):
            row['file_id'] = foreign_key
            if i == 0:
                logger.debug("Keys to insert: {}".format(row.keys()))
            logger.debug("Values to insert: {}".format(row.values()))

        #-- Converts np arrays to native python type...
        #-- This is to allow the database to ingest values as type float
        #-- instead of Decimal Class types in sqlalchemy....
            for key in row:
                if isinstance(row[key], np.generic):
                    logger.debug("casting {} to scalar".format(row[key]))
                    row[key] = np.asscalar(row[key])
                else:
                    continue

                #-- This will check for the instance of rootname that may exist in the table
                #-- Since we ingest FUVA first, then FUVB, we want to eliminate any repeating
                #-- data instances.
                if table.__tablename__ == 'fuv_primary_headers':
                    q = session.query(table.id).filter(table.rootname==row['rootname'])
                    if not session.query(q.exists()).scalar():
                        session.add(table(**row))
                    else:
                        pass
                else:
                    session.add(table(**row))

            session.add(table(**row))

    except (IOError, ValueError) as e:
        #-- Handle missing files
        logger.warning("Exception hit for {}, adding blank entry".format(filename))
        logger.warning(e)
        session.add(table(file_id=foreign_key))

    session.commit()
    session.close()
    engine.dispose()

#-------------------------------------------------------------------------------




# BEGIN INSERTION QUERIES




#-------------------------------------------------------------------------------
def insert_files(**kwargs):
    """Populate the main table of all files in the base directory

    Directly populates the Files table with the full path to all existing files
    located recursively down through the file structure.

    Parameters
    ----------
    data_location : str, optional
        location of the data files to populate the table, defaults to './'

    """

    settings = open_settings()
    Session, engine = load_connection(settings['connection_string'])

    logger.info("Inserting files into {}".format(settings['database']))
    data_location = kwargs.get('data_location', './')
    logger.info("Looking for new files in {}".format(data_location))

    session = Session()
    logger.debug("querying previously found files")

    previous_files = {os.path.join(path, fname) for path, fname in session.query(Files.path, Files.filename)}

    for i, (path, filename) in enumerate(find_all_datasets(data_location, settings['num_cpu'])):
        full_filepath = os.path.join(path, filename)

        if full_filepath in previous_files:
            continue

        logger.debug("NEW: Found {}".format(full_filepath))

        #-- properly formatted HST data should be the first 9 characters
        #-- if this is not the case, insert NULL for this value
        rootname = filename.split('_')[0]

        if not len(rootname) == 9:
            rootname = 'N/A'

        session.add(Files(path=path,
                          filename=filename,
                          rootname=rootname))

        #-- Commit every 20 files to not lose too much progress if
        #-- a failure happens.
        if not i%20:
            session.commit()

    session.commit()
    session.close()

#-------------------------------------------------------------------------------
def populate_darks(num_cpu=1):
    """ Populate the darks table

    """

    logger.info("Adding to Dark table")

    settings = open_settings()
    Session, engine = load_connection(settings['connection_string'])
    session = Session()

    files_to_add = [(result.id, os.path.join(result.path, result.name))
                        for result in session.query(Files).\
                            outerjoin(Headers, Files.rootname == Headers.rootname).\
                            outerjoin(Darks, Files.id == Darks.file_id).\
                            filter(Headers.targname == 'DARK').\
                            filter(Darks.file_id == None).\
                            filter(Files.name.like('%\_corrtag%'))]

    session.close()

    args = [(full_filename, Darks, pull_orbital_info, f_key) for f_key, full_filename in files_to_add]

    logger.info("Found {} files to add".format(len(args)))
    pool = mp.Pool(processes=num_cpu)
    pool.map(mp_insert, args)

#-------------------------------------------------------------------------------
def populate_gain(num_cpu=1):
    """ Populate the cci gain table

    """

    logger.info("adding to gain table")
    settings = open_settings()
    out_dir = os.path.join(settings['monitor_location'], 'CCI')

    Session, engine = load_connection(settings['connection_string'])

    session = Session()

    files_to_add = [(result.id, os.path.join(result.path, result.name))
                        for result in session.query(Files).\
                            outerjoin(Gain, Files.id == Gain.file_id).\
                            filter(or_(Files.name.like('l\_%\_00\____\_cci%'),
                                       Files.name.like('l\_%\_01\____\_cci%'))).\
                            filter(Gain.file_id == None)]
    session.close()

    functions = [functools.partial(insert_with_yield,
                                   filename=filename,
                                   table=Gain,
                                   function=write_and_pull_gainmap,
                                   foreign_key=f_key,
                                   out_dir=out_dir) for f_key, filename in files_to_add]

    logger.info("Found {} files to add".format(len(functions)))
    pool = mp.Pool(processes=num_cpu)
    pool.map(call, functions)
#-------------------------------------------------------------------------------
def populate_lampflash(num_cpu=1):
    """ Populate the lampflash table

    """
    logger.info("adding to lampflash table")

    settings = open_settings()
    Session, engine = load_connection(settings['connection_string'])
    session = Session()

    files_to_add = [(result.id, os.path.join(result.path, result.name))
                        for result in session.query(Files).\
                                filter(or_(Files.name.like('%lampflash%'), (Files.name.like('%_rawacq%')))).\
                                outerjoin(Lampflash, Files.id == Lampflash.file_id).\
                                filter(Lampflash.file_id == None)]
    session.close()
    engine.dispose()

    args = [(full_filename, Lampflash, pull_flashes, f_key) for f_key, full_filename in files_to_add]

    logger.info("Found {} files to add".format(len(args)))
    pool = mp.Pool(processes=num_cpu)
    pool.map(mp_insert, args)

#-------------------------------------------------------------------------------
def populate_stims(num_cpu=1):
    """ Populate the stim table

    """
    logger.info("adding to stim table")

    settings = open_settings()
    Session, engine = load_connection(settings['connection_string'])
    session = Session()

    files_to_add = [(result.id, os.path.join(result.path, result.name))
                        for result in session.query(Files).\
                                filter(Files.name.like('%corrtag\_%')).\
                                outerjoin(Stims, Files.id == Stims.file_id).\
                                filter(Stims.file_id == None)]
    session.close()


    args = [(full_filename, Stims, locate_stims, f_key) for f_key, full_filename in files_to_add]

    logger.info("Found {} files to add".format(len(args)))
    pool = mp.Pool(processes=num_cpu)
    pool.map(mp_insert, args)

#-------------------------------------------------------------------------------
def populate_table(tablename, query_string, function, num_cpu=1):
    logger.info("adding to {} table".format(tablename.__tablename__))

    settings = open_settings()
    Session, engine = load_connection(settings['connection_string'])
    session = Session()


    files_to_add = [(result.id, os.path.join(result.path, result.filename))
                        for result in session.query(Files).\
                                filter(Files.filename.like(query_string)).\
                                outerjoin(tablename, Files.id == tablename.file_id).\
                                filter(tablename.file_id == None)]
    session.close()
    args = [(full_filename, tablename, function, f_key) for f_key, full_filename in files_to_add]

    logger.info("Found {} files to add".format(len(args)))
    pool = mp.Pool(processes=num_cpu)
    pool.map(mp_insert, args)
#-------------------------------------------------------------------------------




# BEGIN KEYWORD LISTS FOR DB.





#-------------------------------------------------------------------------------




# BEGIN DB WRAPPERS




#-------------------------------------------------------------------------------

def cm_delete():
    parser = argparse.ArgumentParser(description='Delete file from all databases.')
    parser.add_argument('filename',
                        type=str,
                        help='search string to delete')
    args = parser.parse_args()

    delete_file_from_all(args.filename)

#-------------------------------------------------------------------------------

def delete_file_from_all(filename):
    """Delete a filename from all databases and directory structure

    Parameters
    ----------
    filename : str
        name of the file, will be pattern-matched with %filename%

    """

    settings = open_settings()
    Session, engine = load_connection(settings['connection_string'])

    session = Session()

    print(filename)
    files_to_remove = [(result.id, os.path.join(result.path, result.name))
                            for result in session.query(Files).\
                                    filter(Files.name.like("""%{}%""".format(filename)))]
    session.close()


    print("Found: ")
    print(files_to_remove)
    for (file_id, file_path) in files_to_remove:
        for table in reversed(Base.metadata.sorted_tables):
            print("Removing {}, {} from {}".format(file_path, file_id, table.name))
            if table.name == 'files':
                q = """DELETE FROM {} WHERE id={}""".format(table.name, file_id)
            else:
                q = """DELETE FROM {} WHERE file_id={}""".format(table.name, file_id)

            engine.execute(text(q))


#-------------------------------------------------------------------------------

def cm_describe():
    parser = argparse.ArgumentParser(description='Show file from all databases.')
    parser.add_argument('filename',
                        type=str,
                        help='search string to show')
    args = parser.parse_args()

    show_file_from_all(args.filename)

#-------------------------------------------------------------------------------

def show_file_from_all(filename):
    """
    """

    settings = open_settings()
    Session, engine = load_connection(settings['connection_string'])

    session = Session()

    print(filename)
    files_to_show = [result.rootname
                            for result in session.query(Files).\
                                    filter(Files.name.like("""%{}%""".format(filename)))]
    session.close()


    print("Found: ")
    print(files_to_show)
    for table in reversed(Base.metadata.sorted_tables):
        if not 'rootname' in table.columns:
            continue

        print("**************************")
        print("Searching {}".format(table.name))
        print("**************************")

        for rootname in set(files_to_show):
            q = """SELECT * FROM {} WHERE rootname LIKE '%{}%'""".format(table.name, rootname)

            results = engine.execute(text(q))

            for i, row in enumerate(results):
                for k in row.keys():
                    print(table.name, rootname, k, row[k])

#-------------------------------------------------------------------------------

def clear_all_databases(settings, nuke=False):
    """Dump all databases of all contents...seriously"""


    #if not raw_input("Are you sure you want to delete everything? Y/N: ") == 'Y':
    #    sys.exit("Not deleting, getting out of here.")

    if nuke:
        settings = open_settings()
        Session, engine = load_connection(settings['connection_string'])

        session = Session()
        for table in reversed(Base.metadata.sorted_tables):
            #if table.name == 'files':
            #    #continue
            #    pass
            try:
                print("Deleting {}".format(table.name))
                session.execute(table.delete())
                session.commit()
            except:
                print("Cannot delete {}".format(table.name))
                pass
    else:
        sys.exit('TO DELETE DB YOU NEED TO SET NUKE FLAG, EXITING')

#-------------------------------------------------------------------------------

def run_all_monitors():
    setup_logging()

    #-- make sure all tables are present
    settings = open_settings()
    Session, engine = load_connection(settings['connection_string'])
    Base.metadata.create_all(engine)

    logger.info("Starting to run all monitors.")

    dark_monitor(settings['monitor_location'])
    cci_monitor()
    stim_monitor()
    osm_monitor()

    logger.info("Finished running all monitors.")

#-------------------------------------------------------------------------------

def setup_logging():
    # create the logging file handler
    logging.basicConfig(filename="cosmos_monitors.log",
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        level=logging.DEBUG)

    #-- handler for STDOUT
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logging.getLogger().addHandler(ch)

#-------------------------------------------------------------------------------

### @db_connect
def clean_slate(settings=None, engine=None, session=None):

    parser = argparse.ArgumentParser()
    parser.add_argument("-n",
                        '--nuke',
                        default=False,
                        help="Parser argument to nuke DB")

    args = parser.parse_args()

    settings = open_settings()
    Session, engine = load_connection(settings['connection_string'])

    clear_all_databases(settings, args.nuke)
    Base.metadata.drop_all(engine, checkfirst=False)
    Base.metadata.create_all(engine)

    ingest_all()

    run_all_monitors()
#-------------------------------------------------------------------------------

def ingest_all():
    setup_logging()

    settings = open_settings()
    Session, engine = load_connection(settings['connection_string'])
    Base.metadata.create_all(engine)

    logger.info("Ingesting all data")


    #-- Ingest all files into DB.
    insert_files(**settings)

    #-------------------------------------------------------
    #-- Populate FUV data

    #-- Populate FUV shared primary header info
    populate_table(fuv_primary_headers, '%_rawtag_a.fits%', fuv_primary_keys, settings['num_cpu'] )
    #-- Does this in 2 steps, checks to make sure that if A shares B rootname then skip.
    populate_table(fuv_primary_headers, '%_rawtag_b.fits%', fuv_primary_keys, settings['num_cpu'] )

    #-- Populate FUV Raw data
    #populate_table(fuva_raw, '%_rawtag_a.fits%', fuva_raw_keys, settings['num_cpu'] )
    #populate_table(fuva_raw, '%_rawtag_b.fits%', fuvb_raw_keys, settings['num_cpu'] )

    #-- Populate FUV Corrtags
    #populate_table(fuva_corr, '%_corrtag_a.fits%', fuva_corr_keys, settings['num_cpu'] )
    #populate_table(fuvb_corr, '%_corrtag_b.fits%', fuvb_corr_keys, settings['num_cpu'] )

    #-- Populate FUV x1d
    #populate_table(fuv_x1d, '%_x1d.fits%', fuva_corr_keys, settings['num_cpu'] )

    #-------------------------------------------------------
    #-- Populate NUV data
    #-- Populate NUV Raw data
    #populate_table(nuv_raw, '%_rawtag.fits%', nuv_raw_keys, settings['num_cpu'] )

    #-- Populate NUV Corrtags
    #populate_table(nuv_corr, '%_corrtag.fits%', nuv_corr_keys, settings['num_cpu'] )

    #-- Populate NUV x1d
    #populate_table(nuv_x1d, '%_x1d.fits%', fuva_corr_keys, settings['num_cpu'] )

    #-------------------------------------------------------
    #-- Populate monitor tables
    #populate_spt(settings['num_cpu'])
    #populate_lampflash(settings['num_cpu'])
    #populate_darks(settings['num_cpu'])
    #populate_gain(settings['num_cpu'])
    #populate_stims(settings['num_cpu'])
    #populate_acqs(settings['num_cpu'])
#-------------------------------------------------------------------------------


#-- NEW KEYWORD DICTIONARIES


#-------------------------------------------------------------------------------
def fuv_primary_keys(filename):
    with fits.open(filename) as hdu:
            keywords = {'filename': hdu[0].header['filename'],
                        'rootname': hdu[0].header['rootname'],
                        'date_obs': hdu[1].header['date-obs'],
                        'imagetyp': hdu[0].header['imagetyp'],
                        'targname': hdu[0].header['targname'],
                        'proposid': hdu[0].header['proposid'],
                        'ra_targ': hdu[0].header['ra_targ'],
                        'dec_targ': hdu[0].header['dec_targ'],
                        'obstype': hdu[0].header['obstype'],
                        'obsmode': hdu[0].header['obsmode'],
                        'exptype': hdu[0].header['exptype'],
                        'postarg1': hdu[0].header['postarg1'],
                        'postarg2': hdu[0].header['postarg2'],
                        'life_adj': hdu[0].header['life_adj'],
                        'fppos': hdu[0].header['fppos'],
                        'exp_num': hdu[0].header['exp_num'],
                        'cenwave': hdu[0].header['cenwave'],
                        'propaper': hdu[0].header['propaper'],
                        'apmpos': hdu[0].header.get('apmpos', 'N/A'),
                        'aperxpos': hdu[0].header.get('aperxpos', -999.9),
                        'aperypos': hdu[0].header.get('aperypos', -999.9),
                        'aperture': hdu[0].header['aperture'],
                        'opt_elem': hdu[0].header['opt_elem'],
                        'extended': hdu[0].header['extended'],
                        }
    return keywords
#-------------------------------------------------------------------------------
