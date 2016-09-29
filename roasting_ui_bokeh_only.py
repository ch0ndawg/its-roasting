from cassandra.cluster import Cluster
from bokeh.io import curdoc
from bokeh.models import ColumnDataSource, Div, Column
from bokeh.plotting import figure, curdoc
from bokeh.resources import CDN
from bokeh.embed import components
from bokeh.util.string import encode_utf8
from bokeh.client import push_session

import numpy as np


dataSource = None
cassSession = Cluster().connect("heatgen")
update(0) # query the database

TOOLS="resize,crosshair,pan,wheel_zoom,box_zoom,reset,box_select,lasso_select"

p = figure(tools=TOOLS, x_range=(-10,10), y_range=(-10,10))
# p.circle(xvals,yvals,radius=0.5,fill_color=colors,fill_alpha=0.6,line_color=None)
p.circle('x', 'y', source=dataSource, radius='radius', color='colors',alpha=0.67)

curdoc().add_periodic_callback(update, 100)
curdoc().title = "Temperature Distribution"

session = push_session(curdoc())

def update(t):
    global dataSource
    global cassSession

    timestep = t/100
    res = cassSession.execute("select * from temps where time = %d" % timestep)
    row_list = list(res)

    yvals = [row_list[j].y_coord for j in range(len(row_list))];
    xvals = [row_list[j].x_coord for j in range(len(row_list))];
    zvals = [row_list[j].temp for j in range(len(row_list))];
    #maxZ = 2*max(zvals)

    heatmap = [
     "#%02x%02x%02x" % (int(255.9*u),0,0) for u in zvals
    ]

    plotInfo = dict(x=xvals,y=yvals,radius=zvals,colors=heatmap)
    if dataSource:
        dataSource.data = plotInfo
    else:
        dataSource = ColumnDataSource(plotInfo)

session.show(p)
session.loop_until_closed()

cassSession.shutdown()
