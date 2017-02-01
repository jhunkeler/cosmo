from __future__ import print_function, absolute_import, division

import os

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import ForeignKey, Column, Index, Integer, String, Float, Boolean, Numeric, BigInteger, Text
from sqlalchemy.dialects import mysql
from sqlalchemy.engine import create_engine
from sqlalchemy.orm import sessionmaker, relationship, backref

try:
    import yaml
except ImportError:
    from .yaml import yaml

__all__ = ['open_settings', 'load_connection']

Base = declarative_base()

#-------------------------------------------------------------------------------

def open_settings(config_file=None):
    """ Parse config file and load settings

    If no config file is supplied, the configuration file will assume to be
    located at '~/configure.yaml'.

    Parameters
    ----------
    config_file : str, optional
        yaml file containing configuration settings.

    Returns
    -------
    settings : dict
        dictionary of all settings

    """

    config_file = config_file or os.path.join(os.environ['HOME'], "configure_dev.yaml")

    with open(config_file, 'r') as f:
        settings = yaml.load(f)

    return settings

#-------------------------------------------------------------------------------

def load_connection(connection_string, echo=False):
    """Create and return a connection to the database given in the
    connection string.

    Parameters
    ----------
    connection_string : str
        A string that points to the database conenction.  The
        connection string is in the following form:
        dialect+driver://username:password@host:port/database
    echo : bool
        Show all SQL produced.

    Returns
    -------
    session : sesson object
        Provides a holding zone for all objects loaded or associated
        with the database.
    engine : engine object
        Provides a source of database connectivity and behavior.
    """

    engine = create_engine(connection_string, echo=echo)
    Session = sessionmaker(bind=engine)

    return Session, engine

#-------------------------------------------------------------------------------

class Files(Base):
    __tablename__ = 'all_files'

    id = Column(Integer, primary_key=True)

    path = Column(String(70))
    filename = Column(String(60))
    rootname = Column(String(9))

    __table_args__ = (Index('idx_fullpath', 'path', 'filename', unique=True), )
    __table_args__ = (Index('idx_rootname', 'rootname'), )
#-------------------------------------------------------------------------------

class Darks(Base):
    __tablename__ = "darks"

    id = Column(Integer, primary_key=True)

    obsname = Column(String(30))
    rootname = Column(String(9))
    detector = Column(String(4))
    date = Column(Float)
    dark = Column(Float)
    ta_dark = Column(Float)
    latitude = Column(Float)
    longitude = Column(Float)
    sun_lat = Column(Float)
    sun_lon = Column(Float)
    temp = Column(Float)

    file_id = Column(Integer, ForeignKey(Files.id))
    #file = relationship("Files", backref=backref('lampflash', order_by=id))

#-------------------------------------------------------------------------------

class Lampflash(Base):
    __tablename__ = 'lampflash'

    id = Column(Integer, primary_key=True)

    date = Column(Float)
    rootname = Column(String(9))
    proposid = Column(Integer)
    detector = Column(String(4))
    segment = Column(String(4))
    opt_elem = Column(String(7))
    cenwave = Column(Integer)
    fppos = Column(Integer)
    lamptab = Column(String(30))
    flash = Column(Integer)
    x_shift = Column(Float)
    y_shift = Column(Float)
    filetype = Column(String(8))
    cal_date = Column(String(30))
    found = Column(Boolean)

    file_id = Column(Integer, ForeignKey(Files.id))
    __table_args__ = (Index('idx_rootname', 'rootname', unique=False), )
    #file = relationship("Files", backref=backref('lampflash', order_by=id))

#-------------------------------------------------------------------------------

class Stims(Base):
    """Record location of all STIM pulses"""
    __tablename__ = "stims"

    id = Column(Integer, primary_key=True)

    time = Column(Float)
    rootname = Column(String(9))
    abs_time = Column(Float)
    stim1_x = Column(Float)
    stim1_y = Column(Float)
    stim2_x = Column(Float)
    stim2_y = Column(Float)
    counts = Column(Float)
    segment = Column(String(4))

    file_id = Column(Integer, ForeignKey(Files.id))
    __table_args__ = (Index('idx_rootname', 'rootname', unique=False), )
    #file = relationship("Files", backref=backref('Stims', order_by=id))

#-------------------------------------------------------------------------------

class Phd(Base):
    __tablename__ = 'phd'

    id = Column(Integer, primary_key=True)

    pha_0 = Column(Integer)
    pha_1 = Column(Integer)
    pha_2 = Column(Integer)
    pha_3 = Column(Integer)
    pha_4 = Column(Integer)
    pha_5 = Column(Integer)
    pha_6 = Column(Integer)
    pha_7 = Column(Integer)
    pha_8 = Column(Integer)
    pha_9 = Column(Integer)
    pha_10 = Column(Integer)
    pha_11 = Column(Integer)
    pha_12 = Column(Integer)
    pha_13 = Column(Integer)
    pha_14 = Column(Integer)
    pha_15 = Column(Integer)
    pha_16 = Column(Integer)
    pha_17 = Column(Integer)
    pha_18 = Column(Integer)
    pha_19 = Column(Integer)
    pha_20 = Column(Integer)
    pha_21 = Column(Integer)
    pha_22 = Column(Integer)
    pha_23 = Column(Integer)
    pha_24 = Column(Integer)
    pha_26 = Column(Integer)
    pha_25 = Column(Integer)
    pha_27 = Column(Integer)
    pha_28 = Column(Integer)
    pha_29 = Column(Integer)
    pha_30 = Column(Integer)
    pha_31 = Column(Integer)

    file_id = Column(Integer, ForeignKey(Files.id))
    #file = relationship("Files", backref=backref('Phd', order_by=id))

#-------------------------------------------------------------------------------

class Gain(Base):
    __tablename__ = 'gain'

    id = Column(BigInteger, primary_key=True)

    x = Column(Integer)
    y = Column(Integer)
    gain = Column(Float)
    counts = Column(Float)
    std = Column(Float)
    segment = Column(String(4))
    dethv = Column(Integer)
    expstart = Column(Float)

    file_id = Column(Integer, ForeignKey(Files.id))
    __table_args__ = (Index('coord', 'x', 'y', unique=False), )
    #file = relationship("Files", backref=backref('Gain', order_by=id))
#-------------------------------------------------------------------------------

class Flagged(Base):
    __tablename__ = 'flagged'

    id = Column(BigInteger, primary_key=True)

    mjd = Column(Float)
    segment = Column(String(4))
    dethv = Column(Integer)
    x = Column(Integer)
    y = Column(Integer)

    file_id = Column(Integer, ForeignKey(Files.id))
    __table_args__ = (Index('coord', 'x', 'y', unique=False), )

#-------------------------------------------------------------------------------

class GainTrends(Base):
    __tablename__ = 'gain_trends'

    id = Column(BigInteger, primary_key=True)

    mjd = Column(Float)
    segment = Column(String(4))
    dethv = Column(Integer)
    x = Column(Integer)
    y = Column(Integer)
    slope = Column(Float)
    intercept = Column(Float)

    file_id = Column(Integer, ForeignKey(Files.id))
    __table_args__ = (Index('coord', 'x', 'y', unique=False), )

#-------------------------------------------------------------------------------

#-- NEW FUV TABLES.

#-------------------------------------------------------------------------------

class fuv_primary_headers(Base):
    'Table of shared keywords for both segments'

    __tablename__ = 'fuv_primary_headers'

    id = Column(Integer, primary_key=True)

    filename = Column(String(50))
    rootname = Column(String(9))
    date_obs = Column(String(10))
    detector = Column(String(5))
    imagetyp = Column(String(10))
    targname = Column(String(50))
    proposid = Column(Integer)
    ra_targ = Column(Float)
    dec_targ = Column(Float)
    pr_inv_l = Column(String(30))
    pr_inv_f = Column(String(20))
    opus_ver = Column(String(20))
    obstype = Column(String(30))
    obsmode = Column(String(30))
    exptype = Column(String(30))
    postarg1 = Column(Integer)
    postarg2 = Column(Integer)
    life_adj = Column(Integer)
    fppos = Column(Integer)
    exp_num = Column(Integer)
    cenwave = Column(Integer)
    propaper = Column(String(10))
    apmpos = Column(String(10))
    aperxpos = Column(Float)
    aperypos = Column(Float)
    aperture = Column(String(15))
    opt_elem = Column(String(15))
    extended = Column(String(5))
    obset_id = Column(String(20))
    asn_id = Column(String(9))
    asn_mtyp = Column(String(10))

    file_id = Column(Integer, ForeignKey(Files.id))
#-------------------------------------------------------------------------------
class fuva_raw_headers(Base):
    'Table of raw fuva data'

    __tablename__ = 'fuva_raw_headers'

    id = Column(Integer, primary_key=True)

    filename = Column(String(50))
    rootname = Column(String(9))
    expstart = Column(Float)
    expend = Column(Float)
    rawtime = Column(Float)
    neventsa = Column(Float)
    deventa = Column(Float)
    feventa = Column(Float)
    hvlevela = Column(Integer)

    file_id = Column(Integer, ForeignKey(Files.id))
#-------------------------------------------------------------------------------
class fuvb_raw_headers(Base):
    'Table of raw fuvb data'

    __tablename__ = 'fuvb_raw_headers'

    id = Column(Integer, primary_key=True)

    filename = Column(String(50))
    rootname = Column(String(9))
    filename = Column(String(70))
    expstart = Column(Float)
    expend = Column(Float)
    rawtime = Column(Float)
    neventsb = Column(Float)
    deventb = Column(Float)
    feventb = Column(Float)
    hvlevelb = Column(Integer)

    file_id = Column(Integer, ForeignKey(Files.id))
#-------------------------------------------------------------------------------
class fuva_corr_headers(Base):
    'Table of corr fuva data'

    __tablename__ = 'fuva_corr_headers'

    id = Column(Integer, primary_key=True)

    filename = Column(String(50))
    rootname = Column(String(9))
    shift1a = Column(Float)
    shift2a = Column(Float)
    sp_loc_a = Column(Float)
    sp_off_a = Column(Float)
    sp_err_a = Column(Float)
    sp_nom_a = Column(Float)
    sp_hgt_a = Column(Integer)
    exptime = Column(Float)

    file_id = Column(Integer, ForeignKey(Files.id))
#-------------------------------------------------------------------------------
class fuvb_corr_headers(Base):
    'Table of corr fuvb data'

    __tablename__ = 'fuvb_corr_headers'

    id = Column(Integer, primary_key=True)

    filename = Column(String(50))
    rootname = Column(String(9))
    shift1b = Column(Float)
    shift2b = Column(Float)
    sp_loc_b = Column(Float)
    sp_off_b = Column(Float)
    sp_err_b = Column(Float)
    sp_nom_b = Column(Float)
    sp_hgt_b = Column(Integer)
    exptime = Column(Float)

    file_id = Column(Integer, ForeignKey(Files.id))
#-------------------------------------------------------------------------------
class fuv_x1d_headers(Base):
    'Table of x1d fuv data'

    __tablename__ = 'fuv_x1d_headers'

    id = Column(Integer, primary_key=True)

    filename = Column(String(50))
    rootname = Column(String(9))
    min_wl = Column(Float)
    max_wl = Column(Float)
    min_flux = Column(Float)
    max_flux = Column(Float)
    mean_flux = Column(Float)
    std_flux = Column(Float)

    file_id = Column(Integer, ForeignKey(Files.id))
#-------------------------------------------------------------------------------

#-- NEW NUV TABLES.

#-------------------------------------------------------------------------------

class nuv_raw_headers(Base):
    'Table of nuv keywords from rawtags'

    __tablename__ = 'nuv_raw_headers'

    id = Column(Integer, primary_key=True)

    filename = Column(String(50))
    rootname = Column(String(9))
    date_obs = Column(String(10))
    detector = Column(String(5))
    imagetyp = Column(String(10))
    targname = Column(String(50))
    proposid = Column(Integer)
    ra_targ = Column(Float)
    dec_targ = Column(Float)
    pr_inv_l = Column(String(30))
    pr_inv_f = Column(String(20))
    opus_ver = Column(String(20))
    obstype = Column(String(30))
    obsmode = Column(String(30))
    exptype = Column(String(30))
    postarg1 = Column(Integer)
    postarg2 = Column(Integer)
    life_adj = Column(Integer)
    fppos = Column(Integer)
    exp_num = Column(Integer)
    cenwave = Column(Integer)
    propaper = Column(String(10))
    apmpos = Column(String(10))
    aperxpos = Column(Float)
    aperypos = Column(Float)
    aperture = Column(String(15))
    opt_elem = Column(String(15))
    extended = Column(String(5))
    obset_id = Column(String(20))
    asn_id = Column(String(9))
    asn_mtyp = Column(String(10))
    expstart = Column(Float)
    expend = Column(Float)
    exptime = Column(Float)
    nevents = Column(Integer)

    file_id = Column(Integer, ForeignKey(Files.id))
#-------------------------------------------------------------------------------

class nuv_corr_headers(Base):
    'Table of nuv keywords from corrtags'

    __tablename__ = 'nuv_corr_headers'

    id = Column(Integer, primary_key=True)

    filename = Column(String(50))
    rootname = Column(String(9))
    shift1a = Column(Float)
    shift1b = Column(Float)
    shift1c = Column(Float)
    shift1b = Column(Float)
    shift2b = Column(Float)
    shift2c = Column(Float)
    sp_loc_a = Column(Float)
    sp_loc_b = Column(Float)
    sp_loc_c = Column(Float)
    sp_off_a = Column(Float)
    sp_off_b = Column(Float)
    sp_off_c = Column(Float)
    sp_nom_a = Column(Float)
    sp_nom_b = Column(Float)
    sp_nom_c = Column(Float)
    sp_hgt_a = Column(Float)
    sp_hgt_b = Column(Float)
    sp_hgt_c = Column(Float)
    exptime = Column(Float)

    file_id = Column(Integer, ForeignKey(Files.id))
#-------------------------------------------------------------------------------
class nuv_x1d_headers(Base):
    'Table of x1d nuv data'

    __tablename__ = 'nuv_x1d_headers'

    id = Column(Integer, primary_key=True)

    filename = Column(String(50))
    rootname = Column(String(9))
    min_wl = Column(Float)
    max_wl = Column(Float)
    min_flux = Column(Float)
    max_flux = Column(Float)
    mean_flux = Column(Float)
    std_flux = Column(Float)

    file_id = Column(Integer, ForeignKey(Files.id))
