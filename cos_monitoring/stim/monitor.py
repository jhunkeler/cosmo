"""
Script to monitor the COS FUV STIM pulses in TIME-TAG observations.

"""

from __future__ import absolute_import, print_function

__author__ = 'Justin Ely'
__status__ = 'Active'

import matplotlib as mpl
mpl.use('agg')
import matplotlib.pyplot as plt
#from mpl_toolkits.mplot3d import Axes3D
import datetime
import shutil
import os
import glob
import numpy as np
np.seterr(divide='ignore')
from astropy.table import Table
from astropy.time import Time
from astropy.io import fits
from scipy.stats import linregress
import logging
logger = logging.getLogger(__name__)

from calcos import ccos

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from ..database.db_tables import open_settings, load_connection
from ..utils import remove_if_there

#-------------------------------------------------------------------------------

def find_center(data):
    """
    Returns (height, x, y, width_x, width_y)
    the gaussian parameters of a 2D distribution by calculating its
    moments

    Parameters
    ----------
    data : array_like
        A COS FUV corrtag image.

    """
    total = data.sum()
    if not total:
        return None, None

    X, Y = np.indices(data.shape)
    x = (X * data).sum() // total
    y = (Y * data).sum() // total

    #-- Measure witdth and height, doesnt have any other use in the monitor.

    #col = data[:, int(y)]
    #width_x = np.sqrt(abs((np.arange(col.size) - y) ** 2 * col).sum() / col.sum())
    #row = data[int(x), :]
    #width_y = np.sqrt(abs((np.arange(row.size) - x) ** 2 * row).sum() / row.sum())
    #height = data.max()

    return y, x

#-------------------------------------------------------------------------------

def brf_positions(brftab, segment, position):
    """
    Gets the search ranges for a stimpulse from
    the given baseline reference table

    Parameters
    ----------
    brftab : str
        path of BRF reference file.
    segment : str
        COS FUV segment
    position : str
        which stim position 'ul' for upper left, 'lr' lower right.

    Returns
    -------
    xmin :
    xmax :
    ymin :
    ymax :
    """
    brf = fits.getdata(brftab)

    if segment == 'FUVA':
        row = 0
    elif segment == 'FUVB':
        row = 1
    else:
        raise ValueError("Segment {} not understood.".format(segment))

    if position == 'ul':
        x_loc = 'SX1'
        y_loc = 'SY1'
    elif position == 'lr':
        x_loc = 'SX2'
        y_loc = 'SY2'

    xcenter = brf[row][x_loc]
    ycenter = brf[row][y_loc]
    xwidth = brf[row]['XWIDTH']
    ywidth = brf[row]['YWIDTH']

    xmin = int(max(xcenter - xwidth, 0))
    xmax = int(min(xcenter + xwidth, 16384))
    ymin = int(max(ycenter - ywidth, 0))
    ymax = int(min(ycenter + ywidth, 1024))

    return xmin, xmax, ymin, ymax

#-------------------------------------------------------------------------------

def find_stims(image, segment, stim, brf_file):
    """
    Find stim positions.

    Parameters
    ----------
    image : array_like

    segment : str
        COS FUV segement
    stim : str
        stim position 'ul' for upper left, 'lr' lower right.
    brf_file : str
        path of BRF reference file

    Returns
    -------
    found_x + x1 : float(?)
        center of stim pulse + scaling factor
    found_y + y1 : float(?)
        center of stim pulse + scaling factor
    """
    x1, x2, y1, y2 = brf_positions(brf_file, segment, stim)
    found_x, found_y = find_center(image[y1:y2, x1:x2])
    if (not found_x) and (not found_y):
        return -999, -999

    return found_x + x1, found_y + y1

#-------------------------------------------------------------------------------

def locate_stims(fits_file, start=0, increment=None, brf_file=None):

    """
    Locate the stim pulse positions

    Parameters
    ----------
    fits_file : str
        COS FUV corrtag path
    start : float
        Start time of observation
    increment : into
        MJD time step.
    brf_file : str
        Path of COS BRFTAB.

    Yields
    ------
    stim_info : dict
        Dictionaty of stim meta data for COSMO
    """
    ### change this to pull brf file from the header if not specified
    if not brf_file:
        try:
            brf_file = fits.getval(fits_file, keyword='brftab', ext=0)
        except KeyError:
            brf_file = os.path.join(os.environ['lref'], 'x1u1459il_brf.fits')

    DAYS_PER_SECOND = 1. / 60. / 60. / 24.

    file_path, file_name = os.path.split(fits_file)

    #-- Begin to gather meta data.
    with fits.open(fits_file) as hdu:
        exptime = hdu[1].header['exptime']
        expstart = hdu[1].header['expstart']
        segment = hdu[0].header['segment']

        stim_info = {'rootname': hdu[0].header['rootname'],
                     'segment': segment}

        #-- See if corrtag has data
        try:
            hdu[1].data
        except:
            yield stim_info
            raise StopIteration

        #-- If the data extension has no length, stop iteration.
        if not len(hdu[1].data):
            yield stim_info
            raise StopIteration

        #-- If increment is not supplied, use the rates supplied by the detector
        if not increment:
            if exptime < 10:
                increment = .03
            elif exptime < 100:
                increment = 1
            else:
                increment = 30

            increment *= 4

        stop = start + increment

        #-- Iterate from start to stop, excluding final bin if smaller than increment
        start_times = np.arange(start, exptime-increment, increment)

        if not len(start_times):
            yield stim_info

        for sub_start in start_times:
            events = hdu['events'].data

            #-- No COS observation has data below ~923
            data_index = np.where((hdu[1].data['time'] >= sub_start) &
                                  (hdu[1].data['time'] <= sub_start+increment))[0]

            events = events[data_index]

            #-- Call for this is x_values, y_values, image to bin to, offset in x
            #ccos.binevents(x, y, array, x_offset, dq, sdqflags, epsilon)
            im = np.zeros((1024, 16384)).astype(np.float32)
            ccos.binevents(events['RAWX'].astype(np.float32),
                           events['RAWY'].astype(np.float32),
                           im,
                           0,
                           events['dq'],
                           0)

            #-- ABS_TIME is the absolute time, MJD, not time in seconds since the exposure started.
            ABS_TIME = expstart + sub_start * DAYS_PER_SECOND

            #-- Send call to locate the stim pulses for upper left and lower right locations.
            found_ul_x, found_ul_y = find_stims(im, segment, 'ul', brf_file)
            found_lr_x, found_lr_y = find_stims(im, segment, 'lr', brf_file)

            #-- Round up and add meta data to dictionary.
            stim_info['time'] = round(sub_start, 5)
            stim_info['abs_time'] = round(ABS_TIME, 5)
            stim_info['stim1_x'] = round(found_ul_x, 3)
            stim_info['stim1_y'] = round(found_ul_y, 3)
            stim_info['stim2_x'] = round(found_lr_x, 3)
            stim_info['stim2_y'] = round(found_lr_y, 3)
            stim_info['counts'] = round(im.sum(), 7)

            yield stim_info

#-----------------------------------------------------

def find_missing():
    """
    Return list of datasets that had a missing stim in
    any time period.

    Parameters
    ----------
    None

    Returns
    -------
    obs_uniq : array_like
        unique list of observation with miss stim pulses
    date_uniq : array_like
        unique list of dates for missing stim pulse observations
    """

    SETTINGS = open_settings()
    Session, engine = load_connection(SETTINGS['connection_string'])

    data = engine.execute("""SELECT headers.rootname,stims.abs_time
                                    FROM stims
                                    JOIN headers ON stims.rootname = headers.rootname
                                    WHERE stims.stim1_x = -999
                                        OR stims.stim1_y = -999
                                        OR stims.stim2_x = -999
                                        OR stims.stim2_y = -999
                                    ORDER BY stims.abs_time""")

    missed_data = [(item[0].strip(), float(item[1])) for item in data]
    all_obs = [item[0] for item in missed_data]
    all_mjd = [item[1] for item in missed_data]

    obs_uniq = list(set(all_obs))

    date_uniq = []
    for obs in obs_uniq:
        min_mjd = all_mjd[all_obs.index(obs)]
        date_uniq.append(min_mjd)

    date_uniq = Time(date_uniq, format='mjd', scale='utc')
    return obs_uniq, date_uniq

#-------------------------------------------------------------------------------

def check_individual(out_dir, connection_string):
    """
    Run tests on each individual datasets.

    Currently checks if any coordinate deviates from the mean
    over the exposure and produces a plot if so.

    Parameters
    ----------
    out_dir : str
        path of output directory
    connection_string : str
        string of database credentials.

    """

    Session, engine = load_connection(connection_string)

    query = """SELECT headers.rootname,
                      headers.segment,
                      headers.proposid,
                      headers.targname,
                      STD(stims.stim1_x) as stim1_xstd,
                      STD(stims.stim1_y) as stim1_ystd,
                      STD(stims.stim2_x) as stim2_xstd,
                      STD(stims.stim2_y) as stim2_ystd
                      FROM stims
                      JOIN headers on stims.rootname = headers.rootname
                      GROUP BY stims.file_id,
                               headers.rootname,
                               headers.segment,
                               headers.proposid,
                               headers.targname
                      HAVING stim1_xstd > 2 OR
                             stim1_ystd > 2 OR
                             stim2_xstd > 2 OR
                             stim2_ystd > 2;"""

    data = []
    for i, row in enumerate(engine.execute(query)):
        if not i:
            keys = row.keys()
        data.append(row.values())

    t = Table(rows=data, names=keys)
    t.write(os.path.join(out_dir, "STIM_problem_rootnames.txt"), delimiter='|', format='ascii.fixed_width')

#-------------------------------------------------------------------------------

def stim_monitor():
    """
    Main function to monitor the stim pulses in COS observations

    1: populate the database
    2: find any datasets with missing stims [send email]
    3: make plots
    4: move plots to webpage
    5: check over individual observations

    Parameters
    ----------
    None
    """

    logger.info("Starting Monitor")

    settings = open_settings()

    webpage_dir = os.path.join(settings['webpage_location'], 'stim')
    monitor_dir = os.path.join(settings['monitor_location'], 'Stims')

    for place in [webpage_dir, monitor_dir]:
        if not os.path.exists(place):
            logger.debug("creating monitor location: {}".format(place))
            os.makedirs(place)

    missing_obs, missing_dates = find_missing()
    send_email(missing_obs, missing_dates)

    make_plots(monitor_dir, settings['connection_string'])

    move_to_web(monitor_dir, webpage_dir)
    check_individual(monitor_dir, settings['connection_string'])

    logger.info("Finished Monitor")

#-------------------------------------------------------------------------------

def send_email(missing_obs, missing_dates):
    """
    email inform to the parties that retrieval and calibration are done

    Parameters
    ----------
    missing_obs : array_like
        a list of observations with missing stim pulse information
    missing_dates : array_like
        a list of dates for missing stim pulse information
    """
    sorted_index = np.argsort(np.array(missing_dates))
    missing_obs = np.array(missing_obs)[sorted_index]
    missing_dates = missing_dates[sorted_index]

    #-- Begin building message.
    date_now = datetime.date.today()
    date_now = Time('%d-%d-%d' % (date_now.year, date_now.month, date_now.day), scale='utc', format='iso')
    date_diff = (date_now - missing_dates).sec / (60. * 60. * 24)

    index = np.where(date_diff < 7)[0]
    message = '--- WARNING ---\n'
    message += 'The following observations had missing stim pulses within the past week:\n'
    message += '-' * 40 + '\n'
    message += ''.join(['{} {}\n'.format(obs, date) for obs, date in zip(missing_obs[index], missing_dates.iso[index])])

    message += '\n\n\n'
    message += 'The following is a complete list of observations with missing stims.\n'
    message += '-' * 40 + '\n'
    message += ''.join(['{} {}\n'.format(obs, date) for obs, date in zip(missing_obs, missing_dates.iso)])

    #-- STScI mail server
    svr_addr = 'smtp.stsci.edu'

    #-- From address
    from_addr = 'mfix@stsci.edu'

    #-- Send to
    #recipients = ['ely@stsci.edu', 'sahnow@stsci.edu', 'penton@stsci.edu', 'sonnentr@stsci.edu']
    recipients = 'mfix@stsci.edu'
    #to_addr = ', '.join(recipients)

    msg = MIMEMultipart()
    msg['Subject'] = 'Stim Monitor Report'
    msg['From'] = from_addr
    #msg['To'] = to_addr
    msg['To'] = recipients

    #-- Send the thing.
    msg.attach(MIMEText(message))
    try:
        s = smtplib.SMTP(svr_addr)
        s.sendmail(from_addr, recipients, msg.as_string())
        s.quit()
    except:
        print("Cannot send email - perhaps you're not on the internal network?")

#-------------------------------------------------------------------------------

def make_plots(out_dir, connection_string):
    """
    Make the overall STIM monitor plots.
    They will all be output to out_dir.

    Parameters
    ----------
    out_dir : str
        path to the output directory
    connection_string :
        string of database credentials.
    """

    plt.ioff()

    brf_file = os.path.join(os.environ['lref'], 'x1u1459il_brf.fits')

    brf = fits.getdata(brf_file, 1)

    Session, engine = load_connection(connection_string)

    plt.figure(1, figsize=(18, 12))
    plt.grid(True)

    #-- Query All data from stims table for FUVA upper left stim pulse
    data = engine.execute("""SELECT stim1_x, stim1_y
                                    FROM stims
                                    JOIN headers on stims.rootname = headers.rootname
                                    WHERE headers.segment = 'FUVA' AND
                                        stims.stim1_x != -999 AND
                                        stims.stim1_y != -999 AND
                                        stims.stim2_x != -999 AND
                                        stims.stim2_y != -999;""")

    #-- Cast data as python list.
    data = [line for line in data]
    plt.subplot(2, 2, 1)

    #-- Seperate list in to x & y coordinates.
    x = [line.stim1_x for line in data]
    y = [line.stim1_y for line in data]

    plt.plot(x, y, 'b.', alpha=.7)
    plt.xlabel('x')
    plt.ylabel('y')
    plt.title('Segment A: Stim A (Upper Left)')

    xcenter = brf[0]['SX1']
    ycenter = brf[0]['SY1']
    xwidth = brf[0]['XWIDTH']
    ywidth = brf[0]['YWIDTH']

    xs = [xcenter - xwidth,
          xcenter + xwidth,
          xcenter + xwidth,
          xcenter - xwidth,
          xcenter - xwidth]
    ys = [ycenter - ywidth,
          ycenter - ywidth,
          ycenter + ywidth,
          ycenter + ywidth,
          ycenter - ywidth]

    plt.plot(xs, ys, color='r', linestyle='--', label='Search Box')
    plt.legend(shadow=True, numpoints=1)
    plt.xlabel('RAWX')
    plt.ylabel('RAWY')
    #plt.set_xlims(xcenter - 2*xwidth, xcenter + 2*xwidth)
    #plt.set_ylims(ycenter - 2*ywidth, ycenter - 2*ywidth)

    #-- Query All data from stims table for FUVA lower right stim pulse
    data = engine.execute("""SELECT stim2_x, stim2_y
                                    FROM stims
                                    JOIN headers on stims.rootname = headers.rootname
                                    WHERE headers.segment = 'FUVA' AND
                                        stims.stim1_x != -999 AND
                                        stims.stim1_y != -999 AND
                                        stims.stim2_x != -999 AND
                                        stims.stim2_y != -999;""")
    data = [line for line in data]

    plt.subplot(2, 2, 2)

    x = [line.stim2_x for line in data]
    y = [line.stim2_y for line in data]

    plt.plot(x, y, 'r.', alpha=.7)
    plt.xlabel('x')
    plt.ylabel('y')
    plt.title('Segment A: Stim B (Lower Right)')

    xcenter = brf[0]['SX2']
    ycenter = brf[0]['SY2']
    xwidth = brf[0]['XWIDTH']
    ywidth = brf[0]['YWIDTH']

    xs = [xcenter - xwidth,
          xcenter + xwidth,
          xcenter + xwidth,
          xcenter - xwidth,
          xcenter - xwidth]
    ys = [ycenter - ywidth,
          ycenter  -ywidth,
          ycenter + ywidth,
          ycenter + ywidth,
          ycenter - ywidth]

    plt.plot(xs, ys, color='r', linestyle='--', label='Search Box')
    plt.legend(shadow=True, numpoints=1)
    plt.xlabel('RAWX')
    plt.ylabel('RAWY')

    #-- Query All data from stims table for FUVB upper left stim pulse
    data = engine.execute("""SELECT stim1_x, stim1_y
                                    FROM stims
                                    JOIN headers on stims.rootname = headers.rootname
                                    WHERE headers.segment = 'FUVB' AND
                                        stims.stim1_x != -999 AND
                                        stims.stim1_y != -999 AND
                                        stims.stim2_x != -999 AND
                                        stims.stim2_y != -999;""")
    data = [line for line in data]

    plt.subplot(2, 2, 3)

    x = [line.stim1_x for line in data]
    y = [line.stim1_y for line in data]

    plt.plot(x, y, 'b.', alpha=.7)
    plt.xlabel('x')
    plt.ylabel('y')
    plt.title('Segment B: Stim A (Upper Left)')

    xcenter = brf[1]['SX1']
    ycenter = brf[1]['SY1']
    xwidth = brf[1]['XWIDTH']
    ywidth = brf[1]['YWIDTH']

    xs = [xcenter - xwidth,
          xcenter + xwidth,
          xcenter + xwidth,
          xcenter - xwidth,
          xcenter - xwidth]
    ys = [ycenter - ywidth,
          ycenter - ywidth,
          ycenter + ywidth,
          ycenter + ywidth,
          ycenter - ywidth]

    plt.plot(xs, ys, color='r', linestyle='--', label='Search Box')
    plt.legend(shadow=True, numpoints=1)
    plt.xlabel('RAWX')
    plt.ylabel('RAWY')

    #-- Query All data from stims table for FUVB lower right stim pulse
    data = engine.execute("""SELECT stim2_x, stim2_y
                                    FROM stims
                                    JOIN headers on stims.rootname = headers.rootname
                                    WHERE headers.segment = 'FUVB' AND
                                        stims.stim1_x != -999 AND
                                        stims.stim1_y != -999 AND
                                        stims.stim2_x != -999 AND
                                        stims.stim2_y != -999;""")
    data = [line for line in data]

    plt.subplot(2, 2, 4)

    x = [line.stim2_x for line in data]
    y = [line.stim2_y for line in data]

    plt.plot(x, y, 'r.', alpha=.7)
    plt.xlabel('x')
    plt.ylabel('y')
    plt.title('Segment B: Stim B (Lower Right)')

    xcenter = brf[1]['SX2']
    ycenter = brf[1]['SY2']
    xwidth = brf[1]['XWIDTH']
    ywidth = brf[1]['YWIDTH']

    xs = [xcenter - xwidth,
          xcenter +xwidth,
          xcenter + xwidth,
          xcenter - xwidth,
          xcenter - xwidth]
    ys = [ycenter - ywidth,
          ycenter - ywidth,
          ycenter + ywidth,
          ycenter + ywidth,
          ycenter - ywidth]

    plt.plot(xs, ys, color='r', linestyle='--', label='Search Box')
    plt.legend(shadow=True, numpoints=1)
    plt.xlabel('RAWX')
    plt.ylabel('RAWY')

    plt.draw()
    remove_if_there(os.path.join(out_dir, 'STIM_locations.png'))
    plt.savefig(os.path.join(out_dir, 'STIM_locations.png'))
    plt.close(1)
    os.chmod(os.path.join(out_dir, 'STIM_locations.png'),0o766)


    for segment in ['FUVA', 'FUVB']:
        fig = plt.figure(2, figsize=(18, 12))
        fig.suptitle('%s coordinate locations with time' % (segment))

        col_names = ['stim1_x', 'stim1_y', 'stim2_x', 'stim2_y']
        titles = ['Upper Left, X', 'Upper Left, Y', 'Lower Right, X', 'Lower Right, Y']

        for i, (column, title) in enumerate(zip(col_names, titles)):
            ax = fig.add_subplot(2, 2, i + 1)
            ax.set_title(title)
            ax.set_xlabel('MJD')
            ax.set_ylabel('Coordinate')

            #-- Retrieve all coordinates and time.
            query = """SELECT stims.abs_time, stims.{}
                              FROM stims
                              JOIN headers ON stims.rootname = headers.rootname
                              WHERE headers.segment = '{}' AND
                                  stims.stim1_x != -999 AND
                                  stims.stim1_y != -999 AND
                                  stims.stim2_x != -999 AND
                                  stims.stim2_y != -999;""".format(column, segment)

            #-- Type cast to python list
            data = [line for line in engine.execute(query)]

            #-- Seperate time and coordinate info.
            times = [line[0] for line in data]
            coords = [line[1] for line in data]
            ax.plot(times, coords, 'o')

        #-- Remove and replace plot if it exists.
        remove_if_there(os.path.join(out_dir, 'STIM_locations_vs_time_%s.png' %
                                                                    (segment)))
        fig.savefig(os.path.join(out_dir, 'STIM_locations_vs_time_%s.png' %
                                                                    (segment)))
        plt.close(fig)
        os.chmod(os.path.join(out_dir, 'STIM_locations_vs_time_%s.png' %
                                                                    (segment)),0o766)


    # ------------------------#
    # strech and midpoint     #
    # ------------------------#
    for segment in ['FUVA', 'FUVB']:
        fig = plt.figure(figsize=(18, 12))
        fig.suptitle("Strech and Midpoint vs time")

        ax1 = fig.add_subplot(2, 2, 1)

        query = """SELECT stims.abs_time, stims.stim2_x - stims.stim1_x as stretch
                          FROM stims
                          JOIN headers ON stims.rootname = headers.rootname
                          WHERE headers.segment = '{}' AND
                              stims.stim1_x != -999 AND
                              stims.stim1_y != -999 AND
                              stims.stim2_x != -999 AND
                              stims.stim2_y != -999;""".format(segment)

        data = [line for line in engine.execute(query)]

        stretch = [line.stretch for line in data]
        times = [line.abs_time for line in data]

        ax1.plot(times, stretch, 'o')
        ax1.set_xlabel('MJD')
        ax1.set_ylabel('Stretch X')

        ax2 = fig.add_subplot(2, 2, 2)

        query = """SELECT stims.abs_time, .5*(stims.stim2_x + stims.stim1_x) as midpoint
                          FROM stims
                          JOIN headers ON stims.rootname = headers.rootname
                          WHERE headers.segment = '{}' AND
                              stims.stim1_x != -999 AND
                              stims.stim1_y != -999 AND
                              stims.stim2_x != -999 AND
                              stims.stim2_y != -999;""".format(segment)

        data = [line for line in engine.execute(query)]
        midpoint = [line.midpoint for line in data]
        times = [line.abs_time for line in data]

        ax2.plot(times, midpoint, 'o')
        ax2.set_xlabel('MJD')
        ax2.set_ylabel('Midpoint X')

        ax3 = fig.add_subplot(2, 2, 3)

        query = """SELECT stims.abs_time, stims.stim2_y - stims.stim1_y as stretch
                          FROM stims
                          JOIN headers ON stims.rootname = headers.rootname
                          WHERE headers.segment = '{}' AND
                              stims.stim1_x != -999 AND
                              stims.stim1_y != -999 AND
                              stims.stim2_x != -999 AND
                              stims.stim2_y != -999;""".format(segment)

        data = [line for line in engine.execute(query)]

        stretch = [line.stretch for line in data]
        times = [line.abs_time for line in data]

        ax3.plot(times, stretch, 'o')
        ax3.set_xlabel('MJD')
        ax3.set_ylabel('Stretch Y')

        ax4 = fig.add_subplot(2, 2, 4)

        query = """SELECT stims.abs_time, .5*(stims.stim2_y + stims.stim1_y) as midpoint
                          FROM stims
                          JOIN headers ON stims.rootname = headers.rootname
                          WHERE headers.segment = '{}' AND
                              stims.stim1_x != -999 AND
                              stims.stim1_y != -999 AND
                              stims.stim2_x != -999 AND
                              stims.stim2_y != -999;""".format(segment)

        ax4.plot(times, midpoint, 'o')
        ax4.set_xlabel('MJD')
        ax4.set_ylabel('Midpoint Y')
        remove_if_there(os.path.join(out_dir, 'STIM_stretch_vs_time_%s.png' %
                     (segment)))
        fig.savefig(
            os.path.join(out_dir, 'STIM_stretch_vs_time_%s.png' %
                         (segment)))
        plt.close(fig)
        os.chmod(os.path.join(out_dir, 'STIM_stretch_vs_time_%s.png' %
                     (segment)),0o766)

    fig = plt.figure(1, figsize=(18, 12))
    ax = fig.add_subplot(2, 2, 1)
    ax.grid(True)

    data = engine.execute("""SELECT stim1_x, stim2_x
                                    FROM stims
                                    JOIN headers on stims.rootname = headers.rootname
                                    WHERE headers.segment = 'FUVA' AND
                                        stims.stim1_x != -999 AND
                                        stims.stim1_y != -999 AND
                                        stims.stim2_x != -999 AND
                                        stims.stim2_y != -999;""")
    data = [line for line in data]

    x1 = [float(line.stim1_x) for line in data]
    x2 = [float(line.stim2_x) for line in data]

    im, nothin1, nothin2 = np.histogram2d(x2, x1, bins=200)  ##reverse coords
    im = np.log(im)
    ax.imshow(im, aspect='auto', interpolation='none')
    ax.set_xlabel('x1')
    ax.set_ylabel('x2')
    ax.set_title('Segment A: X vs X')



    ax = fig.add_subplot(2, 2, 2)
    ax.grid(True)

    data = engine.execute("""SELECT stim1_y, stim2_y
                                    FROM stims
                                    JOIN headers on stims.rootname = headers.rootname
                                    WHERE headers.segment = 'FUVA' AND
                                        stims.stim1_x != -999 AND
                                        stims.stim1_y != -999 AND
                                        stims.stim2_x != -999 AND
                                        stims.stim2_y != -999;""")

    data = [line for line in data]

    y1 = [float(line.stim1_y) for line in data]
    y2 = [float(line.stim2_y) for line in data]

    im, nothin1, nothin2 = np.histogram2d(y2, y1, bins=200)
    im = np.log(im)

    ax.imshow(im, aspect='auto', interpolation='none')
    ax.set_xlabel('y1')
    ax.set_ylabel('y2')
    ax.set_title('Segment A: Y vs Y')

    ax = fig.add_subplot(2, 2, 3)
    ax.grid(True)

    data = engine.execute("""SELECT stim1_x, stim2_x
                                        FROM stims
                                        JOIN headers on stims.rootname = headers.rootname
                                        WHERE headers.segment = 'FUVB' AND
                                            stims.stim1_x != -999 AND
                                            stims.stim1_y != -999 AND
                                            stims.stim2_x != -999 AND
                                            stims.stim2_y != -999;""")

    data = [line for line in data]

    x1 = [float(line.stim1_x) for line in data]
    x2 = [float(line.stim2_x) for line in data]

    im, nothin1, nothin2 = np.histogram2d(x2, x1, bins=200)
    im = np.log(im)
    ax.imshow(im, aspect='auto', interpolation='none')
    ax.set_xlabel('x1')
    ax.set_ylabel('x2')
    ax.set_title('Segment B: X vs X')

    ax = fig.add_subplot(2, 2, 4)
    ax.grid(True)

    data = engine.execute("""SELECT stim1_y, stim2_y
                                        FROM stims
                                        JOIN headers on stims.rootname = headers.rootname
                                        WHERE headers.segment = 'FUVB' AND
                                            stims.stim1_x != -999 AND
                                            stims.stim1_y != -999 AND
                                            stims.stim2_x != -999 AND
                                            stims.stim2_y != -999;""")

    data = [line for line in data]

    y1 = [float(line.stim1_y) for line in data]
    y2 = [float(line.stim2_y) for line in data]

    im, nothin1, nothin2 = np.histogram2d(y2, y1, bins=200)
    im = np.log(im)

    ax.imshow(im, aspect='auto', interpolation='none')
    ax.set_xlabel('y1')
    ax.set_ylabel('y2')
    ax.set_title('Segment B: Y vs Y')

    #fig.colorbar(colors)
    fig.savefig(os.path.join(out_dir, 'STIM_coord_relations_density.png'))
    plt.close(fig)


    """
    print 1
    fig = plt.figure(1, figsize=(18, 12))
    ax = fig.add_subplot(2, 2, 1, projection='3d')
    ax.grid(True)
    x1 = [line[3] for line in data if '_a.fits' in line[0]]
    x2 = [line[5] for line in data if '_a.fits' in line[0]]
    times = [line[1] for line in data if '_a.fits' in line[0]]
    ax.scatter(x1, x2, times, s=5, c=times, alpha=.5, edgecolors='none')
    ax.set_xlabel('x1')
    ax.set_ylabel('x2')
    ax.set_title('Segment A: X vs X')

    print 2
    ax = fig.add_subplot(2, 2, 2, projection='3d')
    ax.grid(True)
    y1 = [line[4] for line in data if '_a.fits' in line[0]]
    y2 = [line[6] for line in data if '_a.fits' in line[0]]
    times = [line[1] for line in data if '_a.fits' in line[0]]
    ax.scatter(y1, y2, times, s=5, c=times, alpha=.5, edgecolors='none')
    slope, intercept = trend(y1, y2)
    print slope, intercept
    #plt.plot( [min(y1), max(y1)], [slope*min(y1)+intercept, slope*max(y1)+intercept], 'y--', lw=3)
    ax.set_xlabel('y1')
    ax.set_ylabel('y2')
    ax.set_title('Segment A: Y vs Y')

    print 3
    ax = fig.add_subplot(2, 2, 3, projection='3d')
    ax.grid(True)
    x1 = [line[3] for line in data if '_b.fits' in line[0]]
    x2 = [line[5] for line in data if '_b.fits' in line[0]]
    times = [line[1] for line in data if '_b.fits' in line[0]]
    ax.scatter(x1, x2, times, s=5, c=times, alpha=.5, edgecolors='none')
    ax.set_xlabel('x1')
    ax.set_ylabel('x2')
    ax.set_title('Segment B: X vs X')

    print 4
    ax = fig.add_subplot(2, 2, 4, projection='3d')
    ax.grid(True)
    y1 = [line[4] for line in data if '_b.fits' in line[0]]
    y2 = [line[6] for line in data if '_b.fits' in line[0]]
    times = [line[1] for line in data if '_b.fits' in line[0]]
    colors = ax.scatter(y1, y2, times, s=5, c=times, alpha=.5, edgecolors='none')
    slope, intercept = trend(y1, y2)
    print slope, intercept
    #plt.plot( [min(y1), max(y1)], [slope*min(y1)+intercept, slope*max(y1)+intercept], 'y--', lw=3)
    ax.set_xlabel('y1')
    ax.set_ylabel('y2')
    ax.set_title('Segment B: Y vs Y')

    fig.subplots_adjust(right=0.8)
    cbar_ax = fig.add_axes([0.85, 0.15, 0.05, 0.7])
    cbar_ax.set_title('MJD')
    fig.colorbar(colors, cax=cbar_ax)
    fig.savefig(os.path.join(out_dir, 'STIM_coord_relations_time.png'))
    plt.close(fig)
    """

#-------------------------------------------------------------------------------

def trend(val1, val2):
    """
    Calculate a linear least-squares regression for two sets of measurements.

    Parameters
    ----------
    val1 : array_like
        set of measurements
    val2 : array_like
        set of measurements

    Returns
    -------
    slope : float
        Slope of linear regression line.
    intercept : float
        Intercept of regression line.
    """
    val1 = np.array(val1)
    val2 = np.array(val2)

    slope, intercept, r_value, p_value, std_err = linregress(val1, val2)

    return slope, intercept

#-------------------------------------------------------------------------------

def move_to_web(data_dir, web_dir):
    """
    Copy output products to web-facing directories.

    Simple function to move created plots in the data_dir
    to the web_dir.  Will move all files that match the string
    STIM*.p* and then change permissions to 777.

    Parameters
    ----------
    data_dir : str
        Location of data
    web_dir : str
        Location of directory for figures to place on website.

    """

    for item in glob.glob(os.path.join(data_dir, 'STIM*.p*')):
        remove_if_there(os.path.join(web_dir, os.path.basename(item)))
        shutil.copy(item, web_dir)
        os.chmod(os.path.join(web_dir, os.path.basename(item)), 0o777)

#-------------------------------------------------------------------------------
