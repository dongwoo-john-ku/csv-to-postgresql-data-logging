from PyQt5 import uic, QtCore, QtWidgets
from PyQt5.QtWidgets import *
from PyQt5.QtCore import pyqtSlot, Qt
from PyQt5.QtGui import QPixmap
import time, datetime, threading, sys
import pandas as pd
from datetime import datetime
import time
import csv
import psycopg2
from sqlalchemy import create_engine

form_class = uic.loadUiType("main.ui")[0]
class myWindow(QMainWindow, form_class):
    def __init__(self):
        super().__init__()
        self.setupUi(self)
        self.pushButton.clicked.connect(self.connectClicked)
        self.pushButton_2.clicked.connect(self.dirPushButtonClicked)
        self.dirInfo = './csv'
        self.label_5.setText(self.dirInfo)

        global stop
        stop = False
        self.btnClicked = False

    def dirPushButtonClicked(self):
        dirInfo = QFileDialog.getExistingDirectory(self)
        self.label_5.setText(dirInfo)
        self.dirInfo = dirInfo

    def connectClicked(self):
        global stop
        if self.btnClicked :
            self.btnClicked = False
            stop = True
            self.pushButton.setText('start')
            self.label_3.setText('paused')
        else:
            self.pushButton.setText('pause')
            self.label_3.setText('started')
            stop = False
            self.btnClicked = True
            t = MyThread(self.dirInfo)
            try:
                t.daemon = True   # Daemon True is necessary
                t.start()
            except:
                self.label_3.setText('Threading Exception!!!')
            else:
                print("Threading Started")

    def updateDisconnect(self):
        self.pushButton.setText('접속')

class MyThread(threading.Thread):
    def __init__(self, dirInfo):
        threading.Thread.__init__(self)
        self.daemon = True

        self.lengthBuffer = 0
        self.dataBuffer = ''
        self.dirInfo = dirInfo

    def run(self):
        while True:
            # break condition is required
            now = datetime.now()
            curDate = str(now.year) +'-' + str(now.month) +'-' + str(now.day)
            # curDate = '2019-12-11'

            if curDate != self.dataBuffer :
                self.dataBuffer = curDate
                self.lengthBuffer = 0

            print(self.dirInfo)
            curDateFileName = self.dirInfo + '/' + curDate + '.csv'
            print(curDateFileName)

            try:
                df = pd.read_csv(curDateFileName)
            except:
                print("no specific csv file")
            else:
                if len(df) > self.lengthBuffer :
                    df["updatetime"] = pd.to_datetime(df["updatetime"])
                    dfAppended = df.iloc[self.lengthBuffer:]
                    self.lengthBuffer = len(df)
                    print("updated")
                    dfAppendedReductionInfo = dfAppended[["updatetime","sensor","distance", "threshold"]]
                    engine = create_engine('postgresql://postgres:welcome1@localhost:5432/postgres')
                    try:
                        dfAppendedReductionInfo.to_sql('plm', engine, if_exists='append', index=False)
                    except:
                        print("error postgres")
                    else:
                        print("saving scss")
                else:
                    print("not updated yet")

            time.sleep(10)
            if stop == True:
                print("Break!")
                break

if __name__ == "__main__":
    app = QApplication(sys.argv)
    myWindow = myWindow()
    myWindow.show()
    sys.exit(app.exec_())
    # app.exec_()
    # run()
