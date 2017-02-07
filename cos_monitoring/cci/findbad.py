from __future__ import absolute_import, print_function

"""
"""

__author__ = 'Justin Ely'
__maintainer__ = 'Justin Ely'
__email__ = 'ely@stsci.edu'
__status__ = 'Active'

import os
import glob
import logging
logger = logging.getLogger(__name__)

from astropy.io import fits
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import multiprocessing as mp
from sqlalchemy.engine import create_engine
from sqlalchemy import and_, distinct
import scipy
from scipy.optimize import leastsq, newton, curve_fit

from .constants import Y_BINNING, X_BINNING
from ..database.db_tables import open_settings, load_connection
from ..database.db_tables import Flagged, GainTrends, Gain

#-------------------------------------------------------------------------------

def time_trends(save_dir):
    logger.debug('Finding trends with time')

    for item in glob.glob(os.path.join(save_dir, 'cumulative_gainmap_*.png')):
        logger.debug("removing old product: {}".format(item))
        os.remove(item)

    settings = open_settings()
    Session, engine = load_connection(settings['connection_string'])
    session = Session()

    #-- Clean out previous results from flagged table
    logger.debug("Deleting flagged table")
    session.query(Flagged).delete()

    #-- Clean out previous results from gain trends table
    logger.debug("Deleting GainTrends table")
    session.query(GainTrends).delete()

    #-- Force commit.
    session.commit()
    session.close()
    engine.dispose()


    Session, engine = load_connection(settings['connection_string'])
    session = Session()

    pool = mp.Pool(processes=settings['num_cpu'])
    logger.debug("looking for segment/dethv combinations")
    all_combos = [(row.segment, row.dethv) for row in session.query(Gain.segment, Gain.dethv).distinct().filter(and_(Gain.segment!='None',
                                                                                                                     Gain.dethv!='None'))]

    session.commit()
    session.close()
    engine.dispose()

    logger.debug("finding bad pixels for all HV/Segments")
    pool.map(find_flagged, all_combos)

    logger.debug("Measuring gain degredation slopes")
    pool.map(measure_slopes, all_combos)


    settings = open_settings()
    Session, engine = load_connection(settings['connection_string'])
    session = Session()
    #-- write projection files
    for (segment, dethv) in all_combos:
        results = session.query(GainTrends).filter(and_(GainTrends.segment==segment,
                                                         GainTrends.dethv==dethv))

        slope_image = np.zeros((1024, 16384))
        intercept_image = np.zeros((1024, 16384))
        bad_image = np.zeros((1024, 16384))

        for row in results:
            y = row.y * Y_BINNING
            x = row.x * X_BINNING
            slope_image[y:y+Y_BINNING, x:x+X_BINNING] = row.slope
            intercept_image[y:y+Y_BINNING, x:x+X_BINNING] = row.intercept

            bad_image[y:y+Y_BINNING, x:x+X_BINNING] = row.mjd

        if slope_image.any():
            logger.debug("Outputing projection files for {} {}".format(segment, dethv))
            write_projection(save_dir, slope_image, intercept_image, bad_image, segment, dethv)

    session.commit()
    session.close()
    engine.dispose()

#-------------------------------------------------------------------------------

def find_flagged(args):
    segment, hvlevel = args

    settings = open_settings()
    Session, engine = load_connection(settings['connection_string'])

    session = Session()

    all_coords = [(row.x, row.y) for row in session.query(Gain.x, Gain.y).distinct(Gain.x, Gain.y).filter(and_(Gain.segment==segment,
                                                                                                     Gain.dethv==hvlevel,
                                                                                                     Gain.gain<=3,
                                                                                                     Gain.counts>=30))]
    logger.debug("{}, {}: found {} superpixels below 3.".format(segment,
                                                         hvlevel,
                                                         len(all_coords)))


    for x, y in all_coords:

        #--filter above and below possible spectral locations
        if (y > 600//Y_BINNING) or (y < 400//Y_BINNING):
            continue

        #-- Nothing bad before 2010,
        #-- and there are some weird gainmaps back there
        #-- filtering out for now.
        results = session.query(Gain).filter(and_(Gain.segment==segment,
                                                  Gain.dethv==hvlevel,
                                                  Gain.x==x,
                                                  Gain.y==y,
                                                  Gain.expstart>55197))

        all_gain = []
        all_counts = []
        all_std = []
        all_expstart = []
        for row in results:
            all_gain.append(row.gain)
            all_counts.append(row.counts)
            all_std.append(row.std)
            all_expstart.append(row.expstart)

        all_gain = np.array(all_gain)
        all_expstart = np.array(all_expstart)

        below_thresh = np.where(all_gain <= 3)[0]

        if len(below_thresh):
            MJD_bad = all_expstart[below_thresh].min()

            MJD_bad = round(MJD_bad, 5)
            session.add(Flagged(mjd=MJD_bad,
                                segment=segment,
                                dethv=hvlevel,
                                x=x,
                                y=y))

    session.commit()
    session.close()
    engine.dispose()

#-------------------------------------------------------------------------------

def measure_slopes(args):
    segment, hvlevel = args

    settings = open_settings()
    Session, engine = load_connection(settings['connection_string'])

    session = Session()


    logger.debug("{}, {}: Measuring gain degredation slopes.".format(segment, hvlevel))

    all_coords = [(row.x, row.y) for row in session.query(Gain.x, Gain.y).distinct(Gain.x, Gain.y).filter(and_(Gain.segment==segment,
                                                                                                               Gain.dethv==hvlevel))]

    for x, y in all_coords:

        #--filter above and below possible spectral locations
        if (y > 600//Y_BINNING) or (y < 400//Y_BINNING):
            continue

        #-- Nothing bad before 2010,
        #-- and there are some weird gainmaps back there
        #-- filtering out for now.
        results = session.query(Gain).filter(and_(Gain.segment==segment,
                                                  Gain.dethv==hvlevel,
                                                  Gain.x==x,
                                                  Gain.y==y,
                                                  Gain.gain>0,
                                                  Gain.expstart>55197))

        all_gain = []
        all_counts = []
        all_expstart = []
        for row in results:
            all_gain.append(row.gain)
            all_counts.append(row.counts)
            all_expstart.append(row.expstart)

        all_gain = np.array(all_gain)
        all_expstart = np.array(all_expstart)

        if not len(all_gain) > 5:
            continue

        sorted_index = all_gain.argsort()
        all_gain = all_gain[sorted_index]
        all_expstart = all_expstart[sorted_index]


        fit, parameters, success = time_fitting(all_expstart, all_gain)

        if success:
            intercept = parameters[1]
            slope = parameters[0]

            f = lambda x, a, b: a * x + b - 3
            fprime = lambda x, a, b: a

            try:
                date_bad = newton(f, all_gain[-1], fprime, args=tuple(parameters), tol=1e-5, maxiter=1000)
            except RuntimeError:
                date_bad = 0


            session.add(GainTrends(mjd=round(date_bad, 5),
                                   segment=segment,
                                   dethv=hvlevel,
                                   x=x,
                                   y=y,
                                   slope=round(slope, 5),
                                   intercept=round(intercept, 5)))

            session.commit()

    session.close()
    engine.dispose()

#-------------------------------------------------------------------------------

def check_rapid_changes(x_values, y_values):
    """Check for rapid changes in gain values.

    Jumps of 5 PHA values within 28 days (~1 month) are considered severe and
    their MJD values will be returned.

    Parameters
    ----------
    x_values : np.ndarray
        MJD values of gain measurements
    y_values : np.ndarray
        gain measurements

    Returns
    -------
    jumps : dates of significant jumps

    """

    gain_thresh = 5
    mjd_thresh = 28
    #Set initial values at ridiculous numbers
    previous_gain = y_values[0]
    previous_mjd = x_values[0]

    jumps = []
    for gain, mjd in zip(y_values, x_values):
        gain_diff = np.abs(previous_gain - gain)
        mjd_diff = np.abs(previous_mjd - mjd)
        if (gain_diff > gain_thresh) and (mjd_diff < mjd_thresh):
            jumps.append(mjd)

        previous_gain = gain
        previous_mjd = mjd

    return jumps

#-------------------------------------------------------------------------------

def write_projection(out_dir, slope_image, intercept_image, bad_image, segment, dethv):
    """Writs a fits file with information useful for post-monitoring analysis.

    Parameters
    ----------
    slope_image : np.ndarray
        2D image of linear gain degredation slopes
    intercept_image : np.ndarray
        2D image of intercepts for the linear gain degredations
    bad_image : np.ndarray
        2D image of the extrapolated date where the gain will drop below 3
    segment : str
        'FUVA' or 'FUVB', COS detector segment of the measurements
    dethv : int
        Detector high-voltage setting of the measurements

    Returns
    -------
        None

    Outputs
    -------
        FITS file with the saved array data.
    """
    try:
        hdu_out = fits.HDUList(fits.PrimaryHDU())
        #hdu_out[0].header.update('TELESCOP', 'HST')
        #hdu_out[0].header.update('INSTRUME', 'COS')
        #hdu_out[0].header.update('DETECTOR', 'FUV')
        #hdu_out[0].header.update('OPT_ELEM', 'ANY')
        #hdu_out[0].header.update('Fsave_dirILETYPE', 'PROJ_BAD')
        #hdu_out[0].header.update('DETHV', dethv)

        #hdu_out[0].header.update('SEGMENT', segment)

        hdu_out[0].header['TELESCOP'] = 'HST'
        hdu_out[0].header['INSTRUME'] = 'COS'
        hdu_out[0].header['DETECTOR'] = 'FUV'
        hdu_out[0].header['OPT_ELEM'] = 'ANY'
        hdu_out[0].header['Fsave_dirILETYPE'] = 'PROJ_BAD'
        hdu_out[0].header['DETHV'] = dethv
        hdu_out[0].header['SEGMENT'] = segment

        #---Ext 1
        hdu_out.append(fits.ImageHDU(data=bad_image))
        hdu_out[1].header.update('EXTNAME', 'PROJBAD')

        #---Ext 2
        hdu_out.append(fits.ImageHDU(data=slope_image))
        hdu_out[2].header.update('EXTNAME', 'SLOPE')

        #---Ext 3
        hdu_out.append(fits.ImageHDU(data=intercept_image))
        hdu_out[3].header.update('EXTNAME', 'INTERCEPT')

        #---Writeout
        hdu_out.writeto(os.path.join(out_dir, 'proj_bad_{}_{}.fits'.format(segment, dethv)) ,clobber=True)
        hdu_out.close()

    except ValueError:
        print('This thing is jacked up {}'.format(os.path.join(out_dir, 'proj_bad_{}_{}.fits'.format(segment, dethv))))
#-------------------------------------------------------------------------------

def time_fitting(x_fit, y_fit):
    """Fit a linear relation to the x_fit and y_fit parameters

    Parameters
    ----------
    x_fit : np.ndarray
        x-values to fit
    y_fit : np.ndarray
        y-values to fit

    Returns
    -------
    fit, parameters, success : tuple
        fit to the values, fit parameters, boolean-success
    """

    x_fit = np.array(x_fit)
    y_fit = np.array(y_fit)

    ###First fit iteration and remove outliers
    POLY_FIT_ORDER = 1

    slope, intercept = scipy.polyfit(x_fit, y_fit, POLY_FIT_ORDER)
    fit = scipy.polyval((slope, intercept), x_fit)
    fit_sigma = fit.std()
    include_index = np.where(np.abs(fit-y_fit) < 1.5*fit_sigma)[0]

    if len(include_index) < 4:
        return None, None, False

    x_fit_clipped = x_fit[include_index]
    y_fit_clipped = y_fit[include_index]

    parameters = scipy.polyfit(x_fit_clipped, y_fit_clipped, POLY_FIT_ORDER)
    fit = scipy.polyval(parameters, x_fit)

    return fit, parameters, True

#-------------------------------------------------------------------------------