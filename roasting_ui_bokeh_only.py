from cassandra.cluster import Cluster
from bokeh.io import curdoc
from bokeh.models import ColumnDataSource, Div, Column
from bokeh.plotting import figure, curdoc, show
from bokeh.resources import CDN
from bokeh.embed import components
from bokeh.util.string import encode_utf8
from bokeh.client import push_session

import numpy as np
import scipy.io as sio # no, not the Scripps Institute of Oceanography

dataSource = None
raw_colors = sio.loadmat('jet-color.mat')
mat_colors = [list(c) for c in list(255.999*raw_colors['jet1024'])]
jet_web_colors = [ "#%02x%02x%02x" % tuple(x) for x in mat_colors ]
ncolors = len(mat_colors)*0.9999

#enter new server here. Shouldn't hard-code it, though, get it as input
cassSession = Cluster(['52.89.254.215']).connect("heatgen")
t=0

def update():
    global dataSource
    global cassSession
    global t

    timestep = t
    t = t+1
    res = cassSession.execute("select * from temps where time = %d" % timestep)

    if not res:
        t=0 # loop
        #return # pause

    row_list = list(res)
    
    # this *could* be made much more efficient with the numpy deserializer
    # but some mysterious stateful behavior behind the scenes prevents this
    # from working (see branch colors-using-numpy)
    xvals = [row_list[j].x_coord for j in range(len(row_list))];
    yvals = [row_list[j].y_coord for j in range(len(row_list))];
    zvals = [row_list[j].temp for j in range(len(row_list))];

    heatmap = [ jet_web_colors[int(u*ncolors)] for u in zvals ]


    plotInfo = dict(x=xvals,y=yvals,radius=zvals,colors=heatmap)
    if dataSource:
        dataSource.data = plotInfo
    else:
        dataSource = ColumnDataSource(plotInfo)


update() # query the database

TOOLS="resize,crosshair,pan,wheel_zoom,box_zoom,reset,box_select,lasso_select"

p = figure(tools=TOOLS, x_range=(-10,10), y_range=(-10,10))
p.circle('x', 'y', source=dataSource, radius='radius', color='colors',alpha=0.5)

curdoc().add_root(p)

curdoc().add_periodic_callback(update, 17)
curdoc().title = "Temperature Distribution"

# session.loop_until_closed()

#cassSession.shutdown()
