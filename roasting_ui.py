from flask import Flask
from cassandra.cluster import Cluster
from bokeh.io import curdoc
from bokeh.models import ColumnDataSource, Div, Column
from bokeh.plotting import figure, output_file
from bokeh.plotting import figure
from bokeh.resources import CDN
from bokeh.embed import file_html

plot = figure()
plot.circle([1,2], [3,4])

html = file_html(plot, CDN, "my plot")
import numpy as np

app = Flask(__name__)

@app.route("/")
def hello():
    return "Hello World!"

@app.route("/heatgen/")

def heatgen():
    session = Cluster().connect("heatgen")
    res = session.execute("select * from temps where time=20")
    row_list = list(res)

    yvals = [row_list[j].y_coord for j in range(len(row_list))];
    xvals = [row_list[j].x_coord for j in range(len(row_list))];
    zvals = [row_list[j].temp for j in range(len(row_list))];
    plotInfo = dict(x=xvals,y=yvals,z=zvals)


    colors = [
     "#%02x%02x%02x" % (int(256*u),0,0) for u in zvals
    ]
    output_file("test_heat.html", title="color_scatter.py example", mode="cdn")
    TOOLS="resize,crosshair,pan,wheel_zoom,box_zoom,reset,box_select,lasso_select"

    p = figure(tools=TOOLS, x_range=(-10,10), y_range=(-10,10))
    p.circle(xvals,yvals,radius=0.5,fill_color=colors,fill_alpha=0.6,line_color=None)
    show(p)


if __name__ == "__main__":
    app.run()
