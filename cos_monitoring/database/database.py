""" Module to populate and interact with the COSMoS database.

.. warning ::
    The functions contained in this module require a configuration
    file to run.  This file gives the connectring string into the
    database, and is needed to run.

"""

from __future__ import print_function, absolute_import, division

from astropy.io import fits
import os
from sqlalchemy import and_, or_, text, MetaData, and_, delete
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
from .db_tables import fuva_raw_headers, fuvb_raw_headers, fuva_corr_headers, fuvb_corr_headers, fuv_x1d_headers
from .db_tables import nuv_raw_headers, nuv_corr_headers, nuv_x1d_headers
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
                q = session.query(table.rootname).filter(table.rootname==row['rootname'])
                if not session.query(q.exists()).scalar():
                    session.add(table(**row))
                    continue
                else:
                    continue

            #-- Because FUV and NUV have same x1d.fits file format, we need to make sure
            #-- they get seperated into seperate tables.

            #-- FUV x1d
            if table.__tablename__ == 'fuv_x1d_headers':
                q = session.query(fuv_primary_headers).filter(and_(fuv_primary_headers.rootname == row['rootname'],
                                                                   fuv_primary_headers.detector == 'FUV'))
                if session.query(q.exists()).scalar():
                    session.add(table(**row))
                    continue
                else:
                    continue

            #-- NUV x1d
            if table.__tablename__ == 'nuv_x1d_headers':
                q = session.query(nuv_raw_headers).filter(and_(nuv_raw_headers.rootname == row['rootname'],
                                                               nuv_raw_headers.detector == 'NUV'))
                if session.query(q.exists()).scalar():
                    session.add(table(**row))
                    continue
                else:
                    continue

            session.add(table(**row))

    except (IOError, ValueError, TypeError) as e:
        #-- Handle missing files

        #-- Since we have a dynamic file system...
        #-- This deals with deleting
        if isinstance(e, IOError):
            delete_file_from_all(filename)
            logger.info("Deleting All Instances of {} in DB Structure".format(filename))
        else:
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
        #-- if this is not the case, insert N/A for this value
        rootname = filename.split('_')[0]

        if not len(rootname) == 9:
            rootname = 'N/A'


        session.add(Files(path=path,
                          filename=filename,
                          rootname=rootname
                          ))

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
                            filter(Files.filename.like('%\_corrtag%'))]

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
                            filter(or_(Files.filename.like('l\_%\_00\____\_cci.fits%'),
                                       Files.filename.like('l\_%\_01\____\_cci.fits%'))).\
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
                                filter(or_(Files.filename.like('%lampflash%'), (Files.filename.like('%_rawacq%')))).\
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

    files_to_add = [(result.id, os.path.join(result.path, result.filename))
                        for result in session.query(Files).\
                                filter(Files.filename.like('%corrtag\_%')).\
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
    connection = engine.connect()

    files_to_remove = [(result.id, os.path.join(result.path, result.filename))
                            for result in session.query(Files).\
                                    filter(Files.filename.like("""%{}%""".format(filename)))]


    session.close()

    for (file_id, file_path) in files_to_remove:
        for table in reversed(Base.metadata.sorted_tables):
            if table.name == 'all_files':
                #sql = """DELETE FROM :table WHERE id=:file_id"""
                sql = """DELETE FROM {} WHERE id={}""".format(table, file_id)
            else:
                #sql = """DELETE FROM :table WHERE file_id=:file_id"""
                sql = """DELETE FROM {} WHERE file_id={}""".format(table, file_id)

            #params = {'table':table.name, 'file_id':file_id}
            engine.execute(text(sql))
            #engine.execute(text(sql))


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
                                    filter(Files.filename.like("""%{}%""".format(filename)))]
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

    #ingest_all()
    #run_all_monitors()
#-------------------------------------------------------------------------------
def clean_files(num_cpu=1):

    """
    clean_files is a temporary fix for the dynamic file system /smov/cos/Data/
    When reprocessing or reference files are updated, new data is pulled from MAST
    and the path contains the reprocessing date. When the date string in the path
    is changed the newly processed data is ingested and the older data remains in the
    database structure. This function queries all of the paths and filenames in the FILES
    table and tries to open them. If they dont exist, any instance of this file will be
    removed from ALL of the database tables which will make room for the newly processed data.

    Parameters
    ----------
    None

    Returns
    -------
    None
    """

    settings = open_settings()
    Session, engine = load_connection(settings['connection_string'])
    session = Session()

    sql = """SELECT path, filename FROM all_files"""

    results = engine.execute(text(sql))

    session.commit()
    session.close()

    #- Check and see if files actually exist
    #-- If false, hold on to those files.
    files_to_remove = [row['filename'] for row in results if not os.path.exists(os.path.join(row['path'], row['filename']))]

    #-- Pool up and delete files and metadata from database.
    pool = mp.Pool(processes=num_cpu)

    if len(files_to_remove):
        pool.map(delete_file_from_all, files_to_remove)

#-------------------------------------------------------------------------------
def ingest_all():
    setup_logging()

    settings = open_settings()
    Session, engine = load_connection(settings['connection_string'])
    Base.metadata.create_all(engine)

    logger.info("Clearing all data")
    clean_files(settings['num_cpu'])
    logger.info("Ingesting all data")


    #-- Ingest all files into DB.
    #insert_files(**settings)

    #-------------------------------------------------------
    #-- Populate FUV data

    #-- Populate FUV shared primary header info
    populate_table(fuv_primary_headers, '%_rawtag_a.fits%', fuv_primary_keys, settings['num_cpu'])
    #-- Does this in 2 steps, checks to make sure that if A shares B rootname then skip.
    populate_table(fuv_primary_headers, '%_rawtag_b.fits%', fuv_primary_keys, settings['num_cpu'])

    #-- Populate FUV Raw data
    populate_table(fuva_raw_headers, '%_rawtag_a.fits%', fuva_raw_keys, settings['num_cpu'])
    populate_table(fuvb_raw_headers, '%_rawtag_b.fits%', fuvb_raw_keys, settings['num_cpu'])

    #-- Populate FUV Corrtags
    populate_table(fuva_corr_headers, '%_corrtag_a.fits%', fuva_corr_keys, settings['num_cpu'])
    populate_table(fuvb_corr_headers, '%_corrtag_b.fits%', fuvb_corr_keys, settings['num_cpu'])

    #-- Populate FUV x1d
    populate_table(fuv_x1d_headers, 'l%\_x1d%', fuv_x1d_keys, settings['num_cpu'])

    #-------------------------------------------------------
    #-- Populate NUV data

    #-- Populate NUV Raw data
    populate_table(nuv_raw_headers, '%_rawtag.fits%', nuv_raw_keys, settings['num_cpu'])

    #-- Populate NUV Corrtags
    populate_table(nuv_corr_headers, '%_corrtag.fits%', nuv_corr_keys, settings['num_cpu'])

    #-- Populate NUV x1d
    populate_table(nuv_x1d_headers, 'l%\_x1d%', nuv_x1d_keys, settings['num_cpu'])

    #-------------------------------------------------------
    #-- Populate monitor tables

    #populate_spt(settings['num_cpu'])
    populate_lampflash(settings['num_cpu'])
    populate_darks(settings['num_cpu'])
    populate_gain(settings['num_cpu'])
    populate_stims(settings['num_cpu'])
    #populate_acqs(settings['num_cpu'])
#-------------------------------------------------------------------------------


#-- NEW FUV KEYWORD DICTIONARIES


#-------------------------------------------------------------------------------
def fuv_primary_keys(filename):
    with fits.open(filename) as hdu:
            keywords = {'filename': hdu[0].header['filename'],
                        'rootname': hdu[0].header['rootname'],
                        'date_obs': hdu[1].header['date-obs'],
                        'detector': hdu[0].header['detector'],
                        'imagetyp': hdu[0].header['imagetyp'],
                        'targname': hdu[0].header['targname'],
                        'proposid': hdu[0].header['proposid'],
                        'ra_targ': hdu[0].header['ra_targ'],
                        'dec_targ': hdu[0].header['dec_targ'],
                        'pr_inv_l': hdu[0].header['pr_inv_l'],
                        'pr_inv_f': hdu[0].header['pr_inv_f'],
                        'opus_ver': hdu[0].header['opus_ver'],
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
                        'obset_id': hdu[0].header['obset_id'],
                        'asn_id': hdu[0].header['asn_id'],
                        'asn_mtyp': hdu[1].header['asn_mtyp']
                        }
    return keywords
#-------------------------------------------------------------------------------
def fuva_raw_keys(filename):
    with fits.open(filename) as hdu:
            keywords = {'filename': hdu[0].header['filename'],
                        'rootname': hdu[0].header['rootname'],
                        'expstart': hdu[1].header['expstart'],
                        'expend': hdu[1].header['expend'],
                        'rawtime': hdu[1].header['rawtime'],
                        'neventsa': hdu[1].header['neventsa'],
                        'deventa': hdu[1].header['deventa'],
                        'feventa': hdu[1].header['feventa'],
                        'hvlevela': hdu[1].header['hvlevela']
                        }
    return keywords
#-------------------------------------------------------------------------------
def fuvb_raw_keys(filename):
    with fits.open(filename) as hdu:
            keywords = {'filename': hdu[0].header['filename'],
                        'rootname': hdu[0].header['rootname'],
                        'expstart': hdu[1].header['expstart'],
                        'expend': hdu[1].header['expend'],
                        'rawtime': hdu[1].header['rawtime'],
                        'neventsb': hdu[1].header['neventsb'],
                        'deventb': hdu[1].header['deventb'],
                        'feventb': hdu[1].header['feventb'],
                        'hvlevelb': hdu[1].header['hvlevelb']
                        }
    return keywords
#-------------------------------------------------------------------------------
def fuva_corr_keys(filename):
    with fits.open(filename) as hdu:
            keywords = {'filename': hdu[0].header['filename'],
                        'rootname': hdu[0].header['rootname'],
                        'shift1a': hdu[1].header['shift1a'],
                        'shift2a': hdu[1].header['shift2a'],
                        'sp_loc_a': hdu[1].header['sp_loc_a'],
                        'sp_off_a': hdu[1].header['sp_off_a'],
                        'sp_err_a': hdu[1].header.get('sp_err_a', -999.9),
                        'sp_nom_a': hdu[1].header['sp_nom_a'],
                        'sp_hgt_a': hdu[1].header['sp_hgt_a'],
                        'exptime': hdu[1].header['exptime']
                        }
    return keywords
#-------------------------------------------------------------------------------
def fuvb_corr_keys(filename):
    with fits.open(filename) as hdu:
            keywords = {'filename': hdu[0].header['filename'],
                        'rootname': hdu[0].header['rootname'],
                        'shift1b': hdu[1].header['shift1b'],
                        'shift2b': hdu[1].header['shift2b'],
                        'sp_loc_b': hdu[1].header['sp_loc_b'],
                        'sp_off_b': hdu[1].header['sp_off_b'],
                        'sp_err_b': hdu[1].header.get('sp_err_b', -999.9),
                        'sp_nom_b': hdu[1].header['sp_nom_b'],
                        'sp_hgt_b': hdu[1].header['sp_hgt_b'],
                        'exptime': hdu[1].header['exptime']
                        }
    return keywords
#-------------------------------------------------------------------------------
def fuv_x1d_keys(filename):
    with fits.open(filename) as hdu:
            keywords = {'filename': hdu[0].header['filename'],
                        'rootname': hdu[0].header['rootname'],
                        'min_wl': np.min(hdu[1].data['wavelength'].ravel()),
                        'max_wl': np.max(hdu[1].data['wavelength'].ravel()),
                        'min_flux': np.min(hdu[1].data['wavelength'].ravel()),
                        'max_flux': np.max(hdu[1].data['wavelength'].ravel()),
                        'mean_flux': np.mean(hdu[1].data['wavelength'].ravel()),
                        'std_flux': np.std(hdu[1].data['wavelength'].ravel())
                        }
    return keywords
#-------------------------------------------------------------------------------


#-- NEW NUV KEYWORD DICTIONARIES


#-------------------------------------------------------------------------------

def nuv_raw_keys(filename):
    with fits.open(filename) as hdu:
            keywords = {'filename': hdu[0].header['filename'],
                        'rootname': hdu[0].header['rootname'],
                        'date_obs': hdu[1].header['date-obs'],
                        'detector': hdu[0].header['detector'],
                        'imagetyp': hdu[0].header['imagetyp'],
                        'targname': hdu[0].header['targname'],
                        'proposid': hdu[0].header['proposid'],
                        'ra_targ': hdu[0].header['ra_targ'],
                        'dec_targ': hdu[0].header['dec_targ'],
                        'pr_inv_l': hdu[0].header['pr_inv_l'],
                        'pr_inv_f': hdu[0].header['pr_inv_f'],
                        'opus_ver': hdu[0].header['opus_ver'],
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
                        'obset_id': hdu[0].header['obset_id'],
                        'asn_id': hdu[0].header['asn_id'],
                        'asn_mtyp': hdu[1].header['asn_mtyp'],
                        'expstart': hdu[1].header['expstart'],
                        'expend': hdu[1].header['expend'],
                        'exptime': hdu[1].header['exptime'],
                        'nevents': hdu[1].header['nevents']
                        }
    return keywords
#-------------------------------------------------------------------------------
def nuv_corr_keys(filename):
    with fits.open(filename) as hdu:
            keywords = {'filename': hdu[0].header['filename'],
                        'rootname': hdu[0].header['rootname'],
                        'shift1a': hdu[1].header['shift1a'],
                        'shift1b': hdu[1].header['shift1b'],
                        'shift1c': hdu[1].header['shift1c'],
                        'shift1b': hdu[1].header['shift1b'],
                        'shift2b': hdu[1].header['shift2b'],
                        'shift2c': hdu[1].header['shift2c'],
                        'sp_loc_a': hdu[1].header['sp_loc_a'],
                        'sp_loc_b': hdu[1].header['sp_loc_b'],
                        'sp_loc_c': hdu[1].header['sp_loc_c'],
                        'sp_off_a': hdu[1].header['sp_off_a'],
                        'sp_off_b': hdu[1].header['sp_off_b'],
                        'sp_off_c': hdu[1].header['sp_off_c'],
                        'sp_nom_a': hdu[1].header['sp_nom_a'],
                        'sp_nom_b': hdu[1].header['sp_nom_b'],
                        'sp_nom_c': hdu[1].header['sp_nom_c'],
                        'sp_hgt_a': hdu[1].header['sp_hgt_a'],
                        'sp_hgt_b': hdu[1].header['sp_hgt_b'],
                        'sp_hgt_c': hdu[1].header['sp_hgt_c'],
                        'exptime': hdu[1].header['exptime']
                        }
    return keywords
#-------------------------------------------------------------------------------
def nuv_x1d_keys(filename):
    with fits.open(filename) as hdu:
            keywords = {'filename': hdu[0].header['filename'],
                        'rootname': hdu[0].header['rootname'],
                        'min_wl': np.min(hdu[1].data['wavelength'].ravel()),
                        'max_wl': np.max(hdu[1].data['wavelength'].ravel()),
                        'min_flux': np.min(hdu[1].data['wavelength'].ravel()),
                        'max_flux': np.max(hdu[1].data['wavelength'].ravel()),
                        'mean_flux': np.mean(hdu[1].data['wavelength'].ravel()),
                        'std_flux': np.std(hdu[1].data['wavelength'].ravel())
                        }
    return keywords
