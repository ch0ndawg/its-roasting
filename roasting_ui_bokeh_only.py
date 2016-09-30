import copy
from cassandra.cluster import Cluster
from cassandra.protocol import NumpyProtocolHandler
from cassandra.query import tuple_factory

from bokeh.io import curdoc
from bokeh.models import ColumnDataSource, Div, Column
from bokeh.plotting import figure, curdoc, show
from bokeh.resources import CDN
from bokeh.embed import components
from bokeh.util.string import encode_utf8
from bokeh.client import push_session

import numpy as np
import pandas as pd
import scipy.io as sio # no, not the Scripps Institute of Oceanography

dataSource = None

# color map information (uses MATLAB jet with 1024 degrees of shading)
raw_colors = sio.loadmat('jet-color.mat')['jet1024']
mat_colors = (255.999*raw_colors).astype(int)
web_jet_colors = np.array(["#%02x%02x%02x" % tuple(x) for x in mat_colors])
ncolors = len(mat_colors)*0.99999

#enter new server here. Shouldn't hard-code it, though, get it as input
cassSession = Cluster(['52.89.254.215']).connect("heatgen")
cassSession.client_protocol_handler = NumpyProtocolHandler
cassSession.row_factory = tuple_factory
t=0

def update():
    global dataSource
    global cassSession
    global t
    global web_jet_colors

    timestep = t
    t = t+1

    # query the database
    res = cassSession.execute("select * from temps where time = %d" % timestep)
    
    # if nothing found
    if not res:
        t=0 # loop (option 1)
        #return # pause (option 2): for live environment

    # extract the values from the query results
    xvals = copy.deepcopy(res[0]['x_coord'].data)
    yvals = copy.deepcopy(res[0]['y_coord'].data)
    zvals = copy.deepcopy(res[0]['temp'].data)

    zcolorind = (zvals*ncolors).astype(int)
    # colors: normalize the data and look it up in a color map
    heatmap = copy.deepcopy(web_jet_colors[zcolorind])


    plotInfo = {'x':xvals,'y':yvals,'radius':zvals, 'colors':heatmap}
    
    if dataSource:
        dataSource.data = copy.deepcopy(plotInfo)
    else:
        dataSource = ColumnDataSource(plotInfo)


update() # query the database

TOOLS="resize,crosshair,pan,wheel_zoom,box_zoom,reset,box_select,lasso_select"

p = figure(tools=TOOLS, x_range=(-10,10), y_range=(-10,10))
p.circle('x', 'y', source=dataSource, radius='radius', color='colors',alpha=0.50)

curdoc().add_root(p)

curdoc().add_periodic_callback(update, 17)
curdoc().title = "Temperature Distribution"


#cassSession.shutdown()
