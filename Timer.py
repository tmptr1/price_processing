from PySide6.QtCore import Signal, QThread, QTime, QTimer

class MyTimer(QThread):
    SetTimeInTableSignal = Signal(int, int, str)
    SetTimeSignal = Signal(str)
    def __init__(self, rowID=None, colID=None, parent=None):
        QThread.__init__(self, parent)
        self.rowId = rowID
        self.colID = colID
        self.time = QTime(0, 0)
        self.time = self.time.addSecs(0)
        self.timer = QTimer()
        if self.rowId or self.colID:
            self.timer.timeout.connect(self.update_time)
        else:
            self.timer.timeout.connect(self.update_total_time)
        self.timer.start(1000)

    def update_time(self):
        self.time = self.time.addSecs(1)
        self.SetTimeInTableSignal.emit(self.rowId, self.colID, self.time.toString('H:mm:ss'))

    def update_total_time(self):
        self.time = self.time.addSecs(1)
        self.SetTimeSignal.emit(self.time.toString('H:mm:ss'))

    def __str__(self):
        return f"{self.time.toString('H:mm:ss')}"