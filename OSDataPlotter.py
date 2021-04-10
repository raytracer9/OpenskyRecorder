#%%
import sqlite3
from sys import path
import pandas as pd
import os
import datashader as ds
from datashader.utils import export_image
import colorcet
from math import ceil, floor
from datetime import date, datetime, timedelta
from param import Filename
import xarray
import ffmpeg 


class OSDataPlotter:

    def __init__(self, 
                 dbLocation:str = "./data.db", 
                 xRange=(-180, 180), yRange=(-90, 90), 
                 startDatetime:datetime=datetime.fromtimestamp(0), 
                 endDatetime:datetime=datetime.fromtimestamp(2^32),
                 xResolution:int = 1024 ):
        
        self.dbLocation = dbLocation
        self.setTimeRange(startDatetime=startDatetime, endDatetime=endDatetime)
        self.setRange(xRange=xRange, yRange=yRange)
        self.setResolutionByX(xResolution)
        self.imgs = []
        pass

    def setTimeRange(self, startDatetime:datetime=datetime.fromtimestamp(0), 
                     endDatetime:datetime=datetime.fromtimestamp(2^32) ):
        
        self.startDatetime = startDatetime
        self.endDatetime = endDatetime

    def setRange(self, xRange=(-180, 180), yRange=(-90, 90), ):
        assert len(xRange) == 2 , "xRange must have a length of 2. (xMin, xMax)"
        self.xRange = xRange
        assert len(yRange) == 2 , "yRange must have a length of 2. (yMin, yMax)"
        self.yRange = yRange 
    
    def setResolutionByX(self, xRes:int):
        # Sets Resolution of the x-Axis. The y-Axis is set according to the ration of the Range.
        xSize = self.xRange[1] - self.xRange[0]
        ySize = self.yRange[1] - self.yRange[0]
        ratio = xSize / ySize
        self.xRes = xRes
        self.yRes = floor(xRes / ratio)
        self.setCanvas()
    
    def setResolutionByY(self, yRes:int):
        # Sets Resolution of the y-Axis. The x-Axis is set according to the ration of the Range.
        xSize = self.xRange[1] - self.xRange[0]
        ySize = self.yRange[1] - self.yRange[0]
        ratio = xSize / ySize
        self.yRes = yRes
        self.xRes = floor(yRes * ratio)
        self.setCanvas()

    def setCanvas(self):
        self.cvs = ds.Canvas(plot_width=self.xRes, plot_height=self.yRes, x_range=self.xRange, y_range=self.yRange) 
    
    def loadData(self):
        stateTimeBegin = int(floor(self.startDatetime.timestamp()))
        stateTimeEnd   = int(ceil(self.endDatetime.timestamp()))
        if (stateTimeEnd < stateTimeBegin): # Validate TimeRange
            raise ValueError(f"stateTimeEnd ({stateTimeEnd}) must be larger than stateTimeBegin ({stateTimeBegin})")

        with sqlite3.connect(self.dbLocation) as con:
            sqlQuery = f"SELECT * FROM states WHERE states.time_state > {stateTimeBegin} AND states.time_state <= {stateTimeEnd}"
            print (sqlQuery)
            self.data = pd.read_sql_query( sqlQuery , con )
            # self.data = pd.read_sql_query( f"SELECT * FROM states WHERE time_state LIMIT 100", con )

    def drawPointsCount(self, cmap=colorcet.gray, how='linear'):
        agg = self.cvs.points(self.data, 'longitude', 'latitude', ds.count())
        self.imgs.append( ds.tf.shade(agg, cmap=cmap, how=how))
    
    def drawPointsTimeTrail(self, cmap=colorcet.gray, how='linear'):
        agg = self.cvs.points(self.data, 'longitude', 'latitude', ds.max('time_state'))
        self.imgs.append( ds.tf.shade(agg, cmap=cmap, how=how))

    def exportStackedImage(self, name:str="dsexport", prefix="", surfix="", sizeInFilename=True, 
                           timestampInFilename=True, format='.png', background='black', howStack="add", outputDir="./"):
        fimg = ds.tf.stack(*self.imgs , how=howStack)
        fimg = ds.tf.set_background(fimg, color=background)
        if sizeInFilename:
            sizeFN = f"{self.xRes}x{self.yRes}"
        else:
            sizeFN = ""
        if timestampInFilename:
            timestampFN = datetime.now().strftime("%y%m%d-%H%M%S_")
        else:
            timestampFN = ""
        filename = f"{prefix}{timestampFN}{name}_{sizeFN}{surfix}"
        export_image(fimg, filename=filename, fmt=format , export_path=outputDir )

    def clearRender(self):
        self.imgs = []

if __name__ == '__main__':
    
    outputDir = "./out1/"
    if ( not os.path.isdir(outputDir) ): os.makedirs(outputDir)
    plotter = OSDataPlotter(dbLocation="./data.db", xResolution=4096, startDatetime=datetime.now()-timedelta(minutes=30), endDatetime=datetime.now())
    plotter.loadData()
    print (plotter.data)
    plotter.drawPointsCount(how='log', cmap=colorcet.gray)
    plotter.drawPointsTimeTrail(how='eq_hist', cmap=colorcet.kbc)
    plotter.exportStackedImage(outputDir=outputDir)


# %%
# plotter.data