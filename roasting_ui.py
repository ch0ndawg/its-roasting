from flask import Flask
from flask import render_template, redirect
from cassandra.cluster import Cluster
from bokeh.io import curdoc
from bokeh.models import ColumnDataSource, Div, Column
from bokeh.plotting import figure, curdoc
from bokeh.resources import CDN
from bokeh.embed import components
from bokeh.util.string import encode_utf8

import numpy as np

app = Flask(__name__)
dataSource = None
session = Cluster().connect("heatgen")

@app.route("/")
def hello():
    return "Hello World!"
@app.route("/heatgen/")
def gotoZero():
    return redirect("/heatgen/0")

@app.route("/heatgen/<int:timestep>")
def heatgen(timestep):
    global dataSource
    global session

    update(timestep*100) # query the database

    TOOLS="resize,crosshair,pan,wheel_zoom,box_zoom,reset,box_select,lasso_select"

    p = figure(tools=TOOLS, x_range=(-10,10), y_range=(-10,10))
    # p.circle(xvals,yvals,radius=0.5,fill_color=colors,fill_alpha=0.6,line_color=None)
    p.circle('x', 'y', source=dataSource, radius='radius', color='colors',alpha=0.67)

    curdoc().add_periodic_callback(update, 100)
    curdoc().title = "Temperature Distribution"

    # translate the script into JavaScript to allow for live callbacks
    script,div = components(p)

    # javactript
    return render_template('roasting.html', script=script, div=div)

def update(t):
    global dataSource
    global session

    timestep = t/100
    res = session.execute("select * from temps where time = %d" % timestep)
    row_list = list(res)

    yvals = [row_list[j].y_coord for j in range(len(row_list))];
    xvals = [row_list[j].x_coord for j in range(len(row_list))];
    zvals = [row_list[j].temp for j in range(len(row_list))];
    #maxZ = 2*max(zvals)

    heatmap = [
     "#%02x%02x%02x" % (int(255.9*u),0,0) for u in zvals
    ]

    if dataSource:
        dataSource.data['x'] = xvals
        dataSource.data['y'] = yvals
        dataSource.data['radius'] = zvals
        dataSource.data['colors'] = heatmap
    else:
        plotInfo = dict(x=xvals,y=yvals,radius=zvals,colors=heatmap)
        dataSource = ColumnDataSource(plotInfo)

if __name__ == "__main__":
    app.run()
