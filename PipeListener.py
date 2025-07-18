from PySide6.QtCore import Signal, QThread

class PipeListener(QThread):
    UpdateinfoTableSignal = Signal(int, str, int, str, bool)
    SetNewRowSignal = Signal(bool)
    ResetTableSignal = Signal(int)
    StopTimerSignal = Signal(int)

    def __init__(self, listener, log, parent=None):
        self.listener = listener
        self.log = log
        QThread.__init__(self, parent)
        self.RowIdDict = dict()

    def run(self):
        conuter = 1
        while True:
            action_type, *other = self.listener.recv()
            if action_type == "add":
                if len(other) == 5:
                    name, price_code, col_id, text, isNewPrice = other
                else:
                    name, price_code, col_id, text = other
                    isNewPrice = False
                # print('add1', name)
                if not self.RowIdDict.get(name, None):
                    self.RowIdDict[name] = conuter
                    conuter += 1
                    self.SetNewRowSignal.emit(True)
                self.UpdateinfoTableSignal.emit(self.RowIdDict[name]-1, price_code, col_id, text, isNewPrice)
                # print(f"{self.RowIdDict=}")
            elif action_type == "new":
                file_count = other[0]
                self.ResetTableSignal.emit(file_count)
                self.RowIdDict = dict()
                conuter = 1
            elif action_type == "end":
                name = other[0]
                if self.RowIdDict.get(name, None):
                    self.StopTimerSignal.emit(self.RowIdDict[name]-1)
            elif action_type == "log":
                id_console_log, log_text, _ = other
                # print(_)
                self.log.add(id_console_log, log_text, _ if _ else None)
            elif action_type == "error":
                id_console_log, error_text, ex_text = other
                self.log.error(id_console_log, error_text, ex_text)