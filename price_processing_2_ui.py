# -*- coding: utf-8 -*-

################################################################################
## Form generated from reading UI file 'price_processing_2.ui'
##
## Created by: Qt User Interface Compiler version 6.9.1
##
## WARNING! All changes made in this file will be lost when recompiling UI file!
################################################################################

from PySide6.QtCore import (QCoreApplication, QDate, QDateTime, QLocale,
    QMetaObject, QObject, QPoint, QRect,
    QSize, QTime, QUrl, Qt)
from PySide6.QtGui import (QBrush, QColor, QConicalGradient, QCursor,
    QFont, QFontDatabase, QGradient, QIcon,
    QImage, QKeySequence, QLinearGradient, QPainter,
    QPalette, QPixmap, QRadialGradient, QTransform)
from PySide6.QtWidgets import (QAbstractScrollArea, QApplication, QCheckBox, QFrame,
    QGridLayout, QGroupBox, QHBoxLayout, QHeaderView,
    QLabel, QMainWindow, QMenuBar, QProgressBar,
    QPushButton, QSizePolicy, QSpacerItem, QSpinBox,
    QStatusBar, QTabWidget, QTableView, QTextBrowser,
    QTimeEdit, QVBoxLayout, QWidget)

class Ui_MainWindow(object):
    def setupUi(self, MainWindow):
        if not MainWindow.objectName():
            MainWindow.setObjectName(u"MainWindow")
        MainWindow.resize(1113, 927)
        self.centralwidget = QWidget(MainWindow)
        self.centralwidget.setObjectName(u"centralwidget")
        self.gridLayout = QGridLayout(self.centralwidget)
        self.gridLayout.setObjectName(u"gridLayout")
        self.tabWidget = QTabWidget(self.centralwidget)
        self.tabWidget.setObjectName(u"tabWidget")
        self.tab_2 = QWidget()
        self.tab_2.setObjectName(u"tab_2")
        self.gridLayout_4 = QGridLayout(self.tab_2)
        self.gridLayout_4.setObjectName(u"gridLayout_4")
        self.gridLayout_3 = QGridLayout()
        self.gridLayout_3.setObjectName(u"gridLayout_3")
        self.horizontalLayout_4 = QHBoxLayout()
        self.horizontalLayout_4.setObjectName(u"horizontalLayout_4")
        self.StartButton_0 = QPushButton(self.tab_2)
        self.StartButton_0.setObjectName(u"StartButton_0")
        font = QFont()
        font.setBold(True)
        self.StartButton_0.setFont(font)
        self.StartButton_0.setStyleSheet(u"")
        icon = QIcon(QIcon.fromTheme(QIcon.ThemeIcon.MediaPlaybackStart))
        self.StartButton_0.setIcon(icon)
        self.StartButton_0.setIconSize(QSize(15, 15))

        self.horizontalLayout_4.addWidget(self.StartButton_0)

        self.PauseCheckBox_0 = QCheckBox(self.tab_2)
        self.PauseCheckBox_0.setObjectName(u"PauseCheckBox_0")
        self.PauseCheckBox_0.setEnabled(False)

        self.horizontalLayout_4.addWidget(self.PauseCheckBox_0)

        self.horizontalSpacer_7 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_4.addItem(self.horizontalSpacer_7)

        self.ToMailFilesDirButton_0 = QPushButton(self.tab_2)
        self.ToMailFilesDirButton_0.setObjectName(u"ToMailFilesDirButton_0")
        icon1 = QIcon(QIcon.fromTheme(QIcon.ThemeIcon.FolderOpen))
        self.ToMailFilesDirButton_0.setIcon(icon1)
        self.ToMailFilesDirButton_0.setIconSize(QSize(15, 15))

        self.horizontalLayout_4.addWidget(self.ToMailFilesDirButton_0)


        self.gridLayout_3.addLayout(self.horizontalLayout_4, 0, 0, 1, 1)

        self.TableLabel_0 = QLabel(self.tab_2)
        self.TableLabel_0.setObjectName(u"TableLabel_0")
        font1 = QFont()
        font1.setPointSize(10)
        font1.setBold(True)
        self.TableLabel_0.setFont(font1)
        self.TableLabel_0.setAlignment(Qt.AlignmentFlag.AlignLeading|Qt.AlignmentFlag.AlignLeft|Qt.AlignmentFlag.AlignVCenter)

        self.gridLayout_3.addWidget(self.TableLabel_0, 1, 0, 1, 1)

        self.MailStatusTable_0 = QTableView(self.tab_2)
        self.MailStatusTable_0.setObjectName(u"MailStatusTable_0")
        self.MailStatusTable_0.setStyleSheet(u"")
        self.MailStatusTable_0.setInputMethodHints(Qt.InputMethodHint.ImhNone)

        self.gridLayout_3.addWidget(self.MailStatusTable_0, 2, 0, 1, 1)

        self.horizontalLayout_5 = QHBoxLayout()
        self.horizontalLayout_5.setObjectName(u"horizontalLayout_5")
        self.UpdateReportButton_0 = QPushButton(self.tab_2)
        self.UpdateReportButton_0.setObjectName(u"UpdateReportButton_0")
        icon2 = QIcon(QIcon.fromTheme(QIcon.ThemeIcon.SystemReboot))
        self.UpdateReportButton_0.setIcon(icon2)
        self.UpdateReportButton_0.setIconSize(QSize(13, 13))

        self.horizontalLayout_5.addWidget(self.UpdateReportButton_0)

        self.ResetMailReportUnloadedButton_0 = QPushButton(self.tab_2)
        self.ResetMailReportUnloadedButton_0.setObjectName(u"ResetMailReportUnloadedButton_0")
        icon3 = QIcon(QIcon.fromTheme(QIcon.ThemeIcon.EditDelete))
        self.ResetMailReportUnloadedButton_0.setIcon(icon3)
        self.ResetMailReportUnloadedButton_0.setIconSize(QSize(15, 15))

        self.horizontalLayout_5.addWidget(self.ResetMailReportUnloadedButton_0)

        self.ResetMailReportButton_0 = QPushButton(self.tab_2)
        self.ResetMailReportButton_0.setObjectName(u"ResetMailReportButton_0")
        self.ResetMailReportButton_0.setIcon(icon3)

        self.horizontalLayout_5.addWidget(self.ResetMailReportButton_0)

        self.OpenReportButton_0 = QPushButton(self.tab_2)
        self.OpenReportButton_0.setObjectName(u"OpenReportButton_0")
        icon4 = QIcon(QIcon.fromTheme(QIcon.ThemeIcon.DocumentPageSetup))
        self.OpenReportButton_0.setIcon(icon4)

        self.horizontalLayout_5.addWidget(self.OpenReportButton_0)

        self.ToMailReportDirButton_0 = QPushButton(self.tab_2)
        self.ToMailReportDirButton_0.setObjectName(u"ToMailReportDirButton_0")
        self.ToMailReportDirButton_0.setIcon(icon1)
        self.ToMailReportDirButton_0.setIconSize(QSize(18, 15))

        self.horizontalLayout_5.addWidget(self.ToMailReportDirButton_0)

        self.horizontalSpacer_10 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_5.addItem(self.horizontalSpacer_10)


        self.gridLayout_3.addLayout(self.horizontalLayout_5, 3, 0, 1, 1)

        self.horizontalLayout_6 = QHBoxLayout()
        self.horizontalLayout_6.setObjectName(u"horizontalLayout_6")
        self.Label_0 = QLabel(self.tab_2)
        self.Label_0.setObjectName(u"Label_0")

        self.horizontalLayout_6.addWidget(self.Label_0)

        self.TimeOfLastReportUpdatelabel_0 = QLabel(self.tab_2)
        self.TimeOfLastReportUpdatelabel_0.setObjectName(u"TimeOfLastReportUpdatelabel_0")
        self.TimeOfLastReportUpdatelabel_0.setFont(font)

        self.horizontalLayout_6.addWidget(self.TimeOfLastReportUpdatelabel_0)

        self.horizontalSpacer_8 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_6.addItem(self.horizontalSpacer_8)


        self.gridLayout_3.addLayout(self.horizontalLayout_6, 4, 0, 1, 1)

        self.horizontalLayout_7 = QHBoxLayout()
        self.horizontalLayout_7.setObjectName(u"horizontalLayout_7")
        self.horizontalSpacer_6 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_7.addItem(self.horizontalSpacer_6)

        self.LogButton_0 = QPushButton(self.tab_2)
        self.LogButton_0.setObjectName(u"LogButton_0")
        icon5 = QIcon(QIcon.fromTheme(QIcon.ThemeIcon.FormatJustifyLeft))
        self.LogButton_0.setIcon(icon5)

        self.horizontalLayout_7.addWidget(self.LogButton_0)


        self.gridLayout_3.addLayout(self.horizontalLayout_7, 5, 0, 1, 1)

        self.ConsoleTextBrowser_0 = QTextBrowser(self.tab_2)
        self.ConsoleTextBrowser_0.setObjectName(u"ConsoleTextBrowser_0")
        self.ConsoleTextBrowser_0.setSizeAdjustPolicy(QAbstractScrollArea.SizeAdjustPolicy.AdjustToContents)

        self.gridLayout_3.addWidget(self.ConsoleTextBrowser_0, 6, 0, 1, 1)

        self.gridLayout_3.setRowStretch(0, 3)
        self.gridLayout_3.setRowStretch(1, 2)
        self.gridLayout_3.setRowStretch(2, 25)
        self.gridLayout_3.setRowStretch(3, 3)
        self.gridLayout_3.setRowStretch(4, 3)
        self.gridLayout_3.setRowStretch(5, 3)
        self.gridLayout_3.setRowStretch(6, 55)

        self.gridLayout_4.addLayout(self.gridLayout_3, 0, 0, 1, 1)

        self.tabWidget.addTab(self.tab_2, "")
        self.tab = QWidget()
        self.tab.setObjectName(u"tab")
        self.gridLayout_5 = QGridLayout(self.tab)
        self.gridLayout_5.setObjectName(u"gridLayout_5")
        self.verticalLayout_2 = QVBoxLayout()
        self.verticalLayout_2.setObjectName(u"verticalLayout_2")
        self.horizontalLayout_2 = QHBoxLayout()
        self.horizontalLayout_2.setObjectName(u"horizontalLayout_2")
        self.StartButton_1 = QPushButton(self.tab)
        self.StartButton_1.setObjectName(u"StartButton_1")
        self.StartButton_1.setFont(font)
        self.StartButton_1.setStyleSheet(u"")
        self.StartButton_1.setIcon(icon)

        self.horizontalLayout_2.addWidget(self.StartButton_1)

        self.PauseCheckBox_1 = QCheckBox(self.tab)
        self.PauseCheckBox_1.setObjectName(u"PauseCheckBox_1")
        self.PauseCheckBox_1.setEnabled(True)

        self.horizontalLayout_2.addWidget(self.PauseCheckBox_1)

        self.horizontalSpacer_11 = QSpacerItem(40, 20, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_2.addItem(self.horizontalSpacer_11)

        self.label = QLabel(self.tab)
        self.label.setObjectName(u"label")

        self.horizontalLayout_2.addWidget(self.label)

        self.FileSizeLimit_spinBox_1 = QSpinBox(self.tab)
        self.FileSizeLimit_spinBox_1.setObjectName(u"FileSizeLimit_spinBox_1")
        self.FileSizeLimit_spinBox_1.setMinimum(1)
        self.FileSizeLimit_spinBox_1.setMaximum(999)
        self.FileSizeLimit_spinBox_1.setValue(1)

        self.horizontalLayout_2.addWidget(self.FileSizeLimit_spinBox_1)

        self.MB_SaveButton_1 = QPushButton(self.tab)
        self.MB_SaveButton_1.setObjectName(u"MB_SaveButton_1")

        self.horizontalLayout_2.addWidget(self.MB_SaveButton_1)

        self.horizontalSpacer_5 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_2.addItem(self.horizontalSpacer_5)

        self.ToFilesDirButton_1 = QPushButton(self.tab)
        self.ToFilesDirButton_1.setObjectName(u"ToFilesDirButton_1")
        self.ToFilesDirButton_1.setIcon(icon1)
        self.ToFilesDirButton_1.setIconSize(QSize(15, 15))

        self.horizontalLayout_2.addWidget(self.ToFilesDirButton_1)


        self.verticalLayout_2.addLayout(self.horizontalLayout_2)

        self.verticalSpacer = QSpacerItem(531, 13, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Expanding)

        self.verticalLayout_2.addItem(self.verticalSpacer)

        self.horizontalLayout = QHBoxLayout()
        self.horizontalLayout.setObjectName(u"horizontalLayout")
        self.horizontalSpacer = QSpacerItem(50, 20, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Minimum)

        self.horizontalLayout.addItem(self.horizontalSpacer)

        self.PriceStatusTableView_1 = QTableView(self.tab)
        self.PriceStatusTableView_1.setObjectName(u"PriceStatusTableView_1")
        self.PriceStatusTableView_1.setStyleSheet(u"")
        self.PriceStatusTableView_1.setInputMethodHints(Qt.InputMethodHint.ImhNone)

        self.horizontalLayout.addWidget(self.PriceStatusTableView_1)

        self.horizontalSpacer_2 = QSpacerItem(50, 20, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Minimum)

        self.horizontalLayout.addItem(self.horizontalSpacer_2)


        self.verticalLayout_2.addLayout(self.horizontalLayout)

        self.verticalSpacer_2 = QSpacerItem(528, 13, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Expanding)

        self.verticalLayout_2.addItem(self.verticalSpacer_2)

        self.gridLayout_2 = QGridLayout()
        self.gridLayout_2.setObjectName(u"gridLayout_2")
        self.horizontalLayout_8 = QHBoxLayout()
        self.horizontalLayout_8.setObjectName(u"horizontalLayout_8")
        self.horizontalSpacer_9 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_8.addItem(self.horizontalSpacer_9)

        self.LogButton_1 = QPushButton(self.tab)
        self.LogButton_1.setObjectName(u"LogButton_1")
        self.LogButton_1.setIcon(icon5)

        self.horizontalLayout_8.addWidget(self.LogButton_1)


        self.gridLayout_2.addLayout(self.horizontalLayout_8, 0, 0, 1, 1)

        self.NotMatchedLabel_1 = QLabel(self.tab)
        self.NotMatchedLabel_1.setObjectName(u"NotMatchedLabel_1")
        self.NotMatchedLabel_1.setFont(font)
        self.NotMatchedLabel_1.setAlignment(Qt.AlignmentFlag.AlignCenter)

        self.gridLayout_2.addWidget(self.NotMatchedLabel_1, 0, 1, 1, 1)

        self.ConsoleTextBrowser_1 = QTextBrowser(self.tab)
        self.ConsoleTextBrowser_1.setObjectName(u"ConsoleTextBrowser_1")
        self.ConsoleTextBrowser_1.setSizeAdjustPolicy(QAbstractScrollArea.SizeAdjustPolicy.AdjustToContents)

        self.gridLayout_2.addWidget(self.ConsoleTextBrowser_1, 1, 0, 1, 1)

        self.verticalLayout = QVBoxLayout()
        self.verticalLayout.setObjectName(u"verticalLayout")
        self.NotMatchedTableView_1 = QTableView(self.tab)
        self.NotMatchedTableView_1.setObjectName(u"NotMatchedTableView_1")

        self.verticalLayout.addWidget(self.NotMatchedTableView_1)

        self.Label_1 = QLabel(self.tab)
        self.Label_1.setObjectName(u"Label_1")

        self.verticalLayout.addWidget(self.Label_1)

        self.TimeOfLastReportUpdatelabel_1 = QLabel(self.tab)
        self.TimeOfLastReportUpdatelabel_1.setObjectName(u"TimeOfLastReportUpdatelabel_1")
        self.TimeOfLastReportUpdatelabel_1.setFont(font)

        self.verticalLayout.addWidget(self.TimeOfLastReportUpdatelabel_1)

        self.horizontalLayout_9 = QHBoxLayout()
        self.horizontalLayout_9.setObjectName(u"horizontalLayout_9")
        self.OpenReportButton_1 = QPushButton(self.tab)
        self.OpenReportButton_1.setObjectName(u"OpenReportButton_1")
        self.OpenReportButton_1.setIcon(icon4)

        self.horizontalLayout_9.addWidget(self.OpenReportButton_1)

        self.ToReportDirButton_1 = QPushButton(self.tab)
        self.ToReportDirButton_1.setObjectName(u"ToReportDirButton_1")
        self.ToReportDirButton_1.setIcon(icon1)
        self.ToReportDirButton_1.setIconSize(QSize(18, 15))

        self.horizontalLayout_9.addWidget(self.ToReportDirButton_1)


        self.verticalLayout.addLayout(self.horizontalLayout_9)

        self.horizontalLayout_10 = QHBoxLayout()
        self.horizontalLayout_10.setObjectName(u"horizontalLayout_10")
        self.UpdateReportButton_1 = QPushButton(self.tab)
        self.UpdateReportButton_1.setObjectName(u"UpdateReportButton_1")
        self.UpdateReportButton_1.setIcon(icon2)
        self.UpdateReportButton_1.setIconSize(QSize(13, 13))

        self.horizontalLayout_10.addWidget(self.UpdateReportButton_1)

        self.ResetPriceReportButton_1 = QPushButton(self.tab)
        self.ResetPriceReportButton_1.setObjectName(u"ResetPriceReportButton_1")
        self.ResetPriceReportButton_1.setIcon(icon3)
        self.ResetPriceReportButton_1.setIconSize(QSize(15, 15))

        self.horizontalLayout_10.addWidget(self.ResetPriceReportButton_1)


        self.verticalLayout.addLayout(self.horizontalLayout_10)


        self.gridLayout_2.addLayout(self.verticalLayout, 1, 1, 2, 1)

        self.horizontalLayout_3 = QHBoxLayout()
        self.horizontalLayout_3.setObjectName(u"horizontalLayout_3")
        self.progressBar_1 = QProgressBar(self.tab)
        self.progressBar_1.setObjectName(u"progressBar_1")
        self.progressBar_1.setMaximumSize(QSize(16777215, 14))
        self.progressBar_1.setStyleSheet(u"")
        self.progressBar_1.setValue(0)
        self.progressBar_1.setTextVisible(False)

        self.horizontalLayout_3.addWidget(self.progressBar_1)

        self.ProgressLabel_1 = QLabel(self.tab)
        self.ProgressLabel_1.setObjectName(u"ProgressLabel_1")

        self.horizontalLayout_3.addWidget(self.ProgressLabel_1)

        self.TotalTimeLabel = QLabel(self.tab)
        self.TotalTimeLabel.setObjectName(u"TotalTimeLabel")
        self.TotalTimeLabel.setFrameShape(QFrame.Shape.NoFrame)

        self.horizontalLayout_3.addWidget(self.TotalTimeLabel)

        self.horizontalSpacer_4 = QSpacerItem(80, 20, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_3.addItem(self.horizontalSpacer_4)

        self.progressBar_1_1 = QProgressBar(self.tab)
        self.progressBar_1_1.setObjectName(u"progressBar_1_1")
        self.progressBar_1_1.setMaximumSize(QSize(16777215, 14))
        self.progressBar_1_1.setStyleSheet(u"")
        self.progressBar_1_1.setValue(0)
        self.progressBar_1_1.setTextVisible(False)

        self.horizontalLayout_3.addWidget(self.progressBar_1_1)

        self.ProgressLabel_1_1 = QLabel(self.tab)
        self.ProgressLabel_1_1.setObjectName(u"ProgressLabel_1_1")

        self.horizontalLayout_3.addWidget(self.ProgressLabel_1_1)

        self.TotalTimeLabel_1_1 = QLabel(self.tab)
        self.TotalTimeLabel_1_1.setObjectName(u"TotalTimeLabel_1_1")
        self.TotalTimeLabel_1_1.setFrameShape(QFrame.Shape.NoFrame)

        self.horizontalLayout_3.addWidget(self.TotalTimeLabel_1_1)


        self.gridLayout_2.addLayout(self.horizontalLayout_3, 2, 0, 2, 1)

        self.horizontalSpacer_3 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.gridLayout_2.addItem(self.horizontalSpacer_3, 3, 1, 1, 1)

        self.gridLayout_2.setColumnStretch(0, 7)
        self.gridLayout_2.setColumnStretch(1, 3)

        self.verticalLayout_2.addLayout(self.gridLayout_2)

        self.verticalLayout_2.setStretch(0, 3)
        self.verticalLayout_2.setStretch(1, 1)
        self.verticalLayout_2.setStretch(2, 10)
        self.verticalLayout_2.setStretch(3, 1)
        self.verticalLayout_2.setStretch(4, 35)

        self.gridLayout_5.addLayout(self.verticalLayout_2, 0, 0, 1, 1)

        self.tabWidget.addTab(self.tab, "")
        self.tab_4 = QWidget()
        self.tab_4.setObjectName(u"tab_4")
        self.gridLayout_12 = QGridLayout(self.tab_4)
        self.gridLayout_12.setObjectName(u"gridLayout_12")
        self.verticalLayout_4 = QVBoxLayout()
        self.verticalLayout_4.setObjectName(u"verticalLayout_4")
        self.horizontalLayout_17 = QHBoxLayout()
        self.horizontalLayout_17.setObjectName(u"horizontalLayout_17")
        self.StartButton_3 = QPushButton(self.tab_4)
        self.StartButton_3.setObjectName(u"StartButton_3")
        self.StartButton_3.setFont(font)
        self.StartButton_3.setStyleSheet(u"")
        self.StartButton_3.setIcon(icon)

        self.horizontalLayout_17.addWidget(self.StartButton_3)

        self.PauseCheckBox_3 = QCheckBox(self.tab_4)
        self.PauseCheckBox_3.setObjectName(u"PauseCheckBox_3")
        self.PauseCheckBox_3.setEnabled(True)

        self.horizontalLayout_17.addWidget(self.PauseCheckBox_3)

        self.horizontalSpacer_29 = QSpacerItem(40, 20, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_17.addItem(self.horizontalSpacer_29)

        self.label_2 = QLabel(self.tab_4)
        self.label_2.setObjectName(u"label_2")

        self.horizontalLayout_17.addWidget(self.label_2)

        self.FileSizeLimit_spinBox_2 = QSpinBox(self.tab_4)
        self.FileSizeLimit_spinBox_2.setObjectName(u"FileSizeLimit_spinBox_2")
        self.FileSizeLimit_spinBox_2.setMinimum(1)
        self.FileSizeLimit_spinBox_2.setMaximum(999)
        self.FileSizeLimit_spinBox_2.setValue(1)

        self.horizontalLayout_17.addWidget(self.FileSizeLimit_spinBox_2)

        self.MB_SaveButton_2 = QPushButton(self.tab_4)
        self.MB_SaveButton_2.setObjectName(u"MB_SaveButton_2")

        self.horizontalLayout_17.addWidget(self.MB_SaveButton_2)

        self.horizontalSpacer_25 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_17.addItem(self.horizontalSpacer_25)

        self.ToFilesDirButton_3 = QPushButton(self.tab_4)
        self.ToFilesDirButton_3.setObjectName(u"ToFilesDirButton_3")
        self.ToFilesDirButton_3.setIcon(icon1)
        self.ToFilesDirButton_3.setIconSize(QSize(15, 15))

        self.horizontalLayout_17.addWidget(self.ToFilesDirButton_3)


        self.verticalLayout_4.addLayout(self.horizontalLayout_17)

        self.verticalSpacer_8 = QSpacerItem(528, 13, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Expanding)

        self.verticalLayout_4.addItem(self.verticalSpacer_8)

        self.horizontalLayout_18 = QHBoxLayout()
        self.horizontalLayout_18.setObjectName(u"horizontalLayout_18")
        self.horizontalSpacer_26 = QSpacerItem(50, 20, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_18.addItem(self.horizontalSpacer_26)

        self.PriceStatusTableView_3 = QTableView(self.tab_4)
        self.PriceStatusTableView_3.setObjectName(u"PriceStatusTableView_3")
        self.PriceStatusTableView_3.setStyleSheet(u"")
        self.PriceStatusTableView_3.setInputMethodHints(Qt.InputMethodHint.ImhNone)

        self.horizontalLayout_18.addWidget(self.PriceStatusTableView_3)

        self.horizontalSpacer_27 = QSpacerItem(50, 20, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_18.addItem(self.horizontalSpacer_27)


        self.verticalLayout_4.addLayout(self.horizontalLayout_18)

        self.verticalSpacer_9 = QSpacerItem(531, 13, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Expanding)

        self.verticalLayout_4.addItem(self.verticalSpacer_9)

        self.gridLayout_11 = QGridLayout()
        self.gridLayout_11.setObjectName(u"gridLayout_11")
        self.horizontalLayout_19 = QHBoxLayout()
        self.horizontalLayout_19.setObjectName(u"horizontalLayout_19")
        self.horizontalSpacer_28 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_19.addItem(self.horizontalSpacer_28)

        self.LogButton_3 = QPushButton(self.tab_4)
        self.LogButton_3.setObjectName(u"LogButton_3")
        self.LogButton_3.setIcon(icon5)

        self.horizontalLayout_19.addWidget(self.LogButton_3)


        self.gridLayout_11.addLayout(self.horizontalLayout_19, 0, 0, 1, 1)

        self.ConsoleTextBrowser_3 = QTextBrowser(self.tab_4)
        self.ConsoleTextBrowser_3.setObjectName(u"ConsoleTextBrowser_3")
        self.ConsoleTextBrowser_3.setSizeAdjustPolicy(QAbstractScrollArea.SizeAdjustPolicy.AdjustToContents)

        self.gridLayout_11.addWidget(self.ConsoleTextBrowser_3, 1, 0, 2, 1)

        self.verticalLayout_6 = QVBoxLayout()
        self.verticalLayout_6.setObjectName(u"verticalLayout_6")
        self.NotMatchedTableView_3 = QTableView(self.tab_4)
        self.NotMatchedTableView_3.setObjectName(u"NotMatchedTableView_3")

        self.verticalLayout_6.addWidget(self.NotMatchedTableView_3)

        self.Label_3 = QLabel(self.tab_4)
        self.Label_3.setObjectName(u"Label_3")

        self.verticalLayout_6.addWidget(self.Label_3)

        self.TimeOfLastReportUpdatelabel_3 = QLabel(self.tab_4)
        self.TimeOfLastReportUpdatelabel_3.setObjectName(u"TimeOfLastReportUpdatelabel_3")
        self.TimeOfLastReportUpdatelabel_3.setFont(font)

        self.verticalLayout_6.addWidget(self.TimeOfLastReportUpdatelabel_3)

        self.horizontalLayout_20 = QHBoxLayout()
        self.horizontalLayout_20.setObjectName(u"horizontalLayout_20")
        self.OpenReportButton_3 = QPushButton(self.tab_4)
        self.OpenReportButton_3.setObjectName(u"OpenReportButton_3")
        self.OpenReportButton_3.setIcon(icon4)

        self.horizontalLayout_20.addWidget(self.OpenReportButton_3)

        self.ToReportDirButton_3 = QPushButton(self.tab_4)
        self.ToReportDirButton_3.setObjectName(u"ToReportDirButton_3")
        self.ToReportDirButton_3.setIcon(icon1)
        self.ToReportDirButton_3.setIconSize(QSize(18, 15))

        self.horizontalLayout_20.addWidget(self.ToReportDirButton_3)


        self.verticalLayout_6.addLayout(self.horizontalLayout_20)

        self.horizontalLayout_21 = QHBoxLayout()
        self.horizontalLayout_21.setObjectName(u"horizontalLayout_21")
        self.UpdateReportButton_3 = QPushButton(self.tab_4)
        self.UpdateReportButton_3.setObjectName(u"UpdateReportButton_3")
        self.UpdateReportButton_3.setIcon(icon2)
        self.UpdateReportButton_3.setIconSize(QSize(13, 13))

        self.horizontalLayout_21.addWidget(self.UpdateReportButton_3)

        self.ResetPriceReportButton_3 = QPushButton(self.tab_4)
        self.ResetPriceReportButton_3.setObjectName(u"ResetPriceReportButton_3")
        self.ResetPriceReportButton_3.setIcon(icon3)
        self.ResetPriceReportButton_3.setIconSize(QSize(15, 15))

        self.horizontalLayout_21.addWidget(self.ResetPriceReportButton_3)


        self.verticalLayout_6.addLayout(self.horizontalLayout_21)


        self.gridLayout_11.addLayout(self.verticalLayout_6, 2, 1, 1, 1)

        self.horizontalLayout_22 = QHBoxLayout()
        self.horizontalLayout_22.setObjectName(u"horizontalLayout_22")
        self.progressBar_3 = QProgressBar(self.tab_4)
        self.progressBar_3.setObjectName(u"progressBar_3")
        self.progressBar_3.setMaximumSize(QSize(16777215, 14))
        self.progressBar_3.setStyleSheet(u"")
        self.progressBar_3.setValue(0)
        self.progressBar_3.setTextVisible(False)

        self.horizontalLayout_22.addWidget(self.progressBar_3)

        self.ProgressLabel_3 = QLabel(self.tab_4)
        self.ProgressLabel_3.setObjectName(u"ProgressLabel_3")

        self.horizontalLayout_22.addWidget(self.ProgressLabel_3)

        self.TotalTimeLabel_3 = QLabel(self.tab_4)
        self.TotalTimeLabel_3.setObjectName(u"TotalTimeLabel_3")
        self.TotalTimeLabel_3.setFrameShape(QFrame.Shape.NoFrame)

        self.horizontalLayout_22.addWidget(self.TotalTimeLabel_3)

        self.horizontalSpacer_32 = QSpacerItem(80, 20, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_22.addItem(self.horizontalSpacer_32)

        self.progressBar_3_1 = QProgressBar(self.tab_4)
        self.progressBar_3_1.setObjectName(u"progressBar_3_1")
        self.progressBar_3_1.setMaximumSize(QSize(16777215, 14))
        self.progressBar_3_1.setStyleSheet(u"")
        self.progressBar_3_1.setValue(0)
        self.progressBar_3_1.setTextVisible(False)

        self.horizontalLayout_22.addWidget(self.progressBar_3_1)

        self.ProgressLabel_3_1 = QLabel(self.tab_4)
        self.ProgressLabel_3_1.setObjectName(u"ProgressLabel_3_1")

        self.horizontalLayout_22.addWidget(self.ProgressLabel_3_1)

        self.TotalTimeLabel_3_1 = QLabel(self.tab_4)
        self.TotalTimeLabel_3_1.setObjectName(u"TotalTimeLabel_3_1")
        self.TotalTimeLabel_3_1.setFrameShape(QFrame.Shape.NoFrame)

        self.horizontalLayout_22.addWidget(self.TotalTimeLabel_3_1)


        self.gridLayout_11.addLayout(self.horizontalLayout_22, 3, 0, 1, 1)

        self.horizontalSpacer_30 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.gridLayout_11.addItem(self.horizontalSpacer_30, 3, 1, 1, 1)

        self.NotMatchedLabel_3 = QLabel(self.tab_4)
        self.NotMatchedLabel_3.setObjectName(u"NotMatchedLabel_3")
        self.NotMatchedLabel_3.setFont(font)
        self.NotMatchedLabel_3.setAlignment(Qt.AlignmentFlag.AlignCenter)

        self.gridLayout_11.addWidget(self.NotMatchedLabel_3, 0, 1, 1, 1)

        self.gridLayout_11.setColumnStretch(0, 7)
        self.gridLayout_11.setColumnStretch(1, 3)

        self.verticalLayout_4.addLayout(self.gridLayout_11)

        self.verticalLayout_4.setStretch(0, 3)
        self.verticalLayout_4.setStretch(1, 1)
        self.verticalLayout_4.setStretch(2, 10)
        self.verticalLayout_4.setStretch(3, 1)
        self.verticalLayout_4.setStretch(4, 35)

        self.gridLayout_12.addLayout(self.verticalLayout_4, 0, 0, 1, 1)

        self.tabWidget.addTab(self.tab_4, "")
        self.tab_5 = QWidget()
        self.tab_5.setObjectName(u"tab_5")
        self.gridLayout_16 = QGridLayout(self.tab_5)
        self.gridLayout_16.setObjectName(u"gridLayout_16")
        self.verticalLayout_9 = QVBoxLayout()
        self.verticalLayout_9.setSpacing(6)
        self.verticalLayout_9.setObjectName(u"verticalLayout_9")
        self.horizontalLayout_25 = QHBoxLayout()
        self.horizontalLayout_25.setObjectName(u"horizontalLayout_25")
        self.StartButton_4 = QPushButton(self.tab_5)
        self.StartButton_4.setObjectName(u"StartButton_4")
        self.StartButton_4.setFont(font)
        self.StartButton_4.setStyleSheet(u"")
        self.StartButton_4.setIcon(icon)

        self.horizontalLayout_25.addWidget(self.StartButton_4)

        self.PauseCheckBox_4 = QCheckBox(self.tab_5)
        self.PauseCheckBox_4.setObjectName(u"PauseCheckBox_4")
        self.PauseCheckBox_4.setEnabled(True)

        self.horizontalLayout_25.addWidget(self.PauseCheckBox_4)

        self.SendCheckBox_4 = QCheckBox(self.tab_5)
        self.SendCheckBox_4.setObjectName(u"SendCheckBox_4")

        self.horizontalLayout_25.addWidget(self.SendCheckBox_4)

        self.horizontalSpacer_34 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_25.addItem(self.horizontalSpacer_34)

        self.ToFilesDirButton_4 = QPushButton(self.tab_5)
        self.ToFilesDirButton_4.setObjectName(u"ToFilesDirButton_4")
        self.ToFilesDirButton_4.setIcon(icon1)
        self.ToFilesDirButton_4.setIconSize(QSize(15, 15))

        self.horizontalLayout_25.addWidget(self.ToFilesDirButton_4)


        self.verticalLayout_9.addLayout(self.horizontalLayout_25)

        self.verticalSpacer_12 = QSpacerItem(20, 40, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Expanding)

        self.verticalLayout_9.addItem(self.verticalSpacer_12)

        self.gridLayout_15 = QGridLayout()
        self.gridLayout_15.setObjectName(u"gridLayout_15")
        self.horizontalLayout_24 = QHBoxLayout()
        self.horizontalLayout_24.setObjectName(u"horizontalLayout_24")
        self.horizontalSpacer_33 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_24.addItem(self.horizontalSpacer_33)

        self.LogButton_4 = QPushButton(self.tab_5)
        self.LogButton_4.setObjectName(u"LogButton_4")
        self.LogButton_4.setIcon(icon5)

        self.horizontalLayout_24.addWidget(self.LogButton_4)


        self.gridLayout_15.addLayout(self.horizontalLayout_24, 0, 0, 1, 1)

        self.NotMatchedLabel_4 = QLabel(self.tab_5)
        self.NotMatchedLabel_4.setObjectName(u"NotMatchedLabel_4")
        self.NotMatchedLabel_4.setFont(font)
        self.NotMatchedLabel_4.setAlignment(Qt.AlignmentFlag.AlignCenter)

        self.gridLayout_15.addWidget(self.NotMatchedLabel_4, 0, 1, 1, 1)

        self.verticalLayout_8 = QVBoxLayout()
        self.verticalLayout_8.setObjectName(u"verticalLayout_8")
        self.ConsoleTextBrowser_4 = QTextBrowser(self.tab_5)
        self.ConsoleTextBrowser_4.setObjectName(u"ConsoleTextBrowser_4")
        self.ConsoleTextBrowser_4.setSizeAdjustPolicy(QAbstractScrollArea.SizeAdjustPolicy.AdjustToContents)

        self.verticalLayout_8.addWidget(self.ConsoleTextBrowser_4)

        self.horizontalLayout_26 = QHBoxLayout()
        self.horizontalLayout_26.setObjectName(u"horizontalLayout_26")
        self.progressBar_4 = QProgressBar(self.tab_5)
        self.progressBar_4.setObjectName(u"progressBar_4")
        self.progressBar_4.setMaximumSize(QSize(16777215, 14))
        self.progressBar_4.setStyleSheet(u"")
        self.progressBar_4.setValue(0)
        self.progressBar_4.setTextVisible(False)

        self.horizontalLayout_26.addWidget(self.progressBar_4)

        self.ProgressLabel_4 = QLabel(self.tab_5)
        self.ProgressLabel_4.setObjectName(u"ProgressLabel_4")

        self.horizontalLayout_26.addWidget(self.ProgressLabel_4)

        self.TotalTimeLabel_4 = QLabel(self.tab_5)
        self.TotalTimeLabel_4.setObjectName(u"TotalTimeLabel_4")
        self.TotalTimeLabel_4.setFrameShape(QFrame.Shape.NoFrame)

        self.horizontalLayout_26.addWidget(self.TotalTimeLabel_4)


        self.verticalLayout_8.addLayout(self.horizontalLayout_26)


        self.gridLayout_15.addLayout(self.verticalLayout_8, 1, 0, 1, 1)

        self.verticalLayout_7 = QVBoxLayout()
        self.verticalLayout_7.setObjectName(u"verticalLayout_7")
        self.FinalPriceSendTableView_4 = QTableView(self.tab_5)
        self.FinalPriceSendTableView_4.setObjectName(u"FinalPriceSendTableView_4")

        self.verticalLayout_7.addWidget(self.FinalPriceSendTableView_4)

        self.Label_4 = QLabel(self.tab_5)
        self.Label_4.setObjectName(u"Label_4")

        self.verticalLayout_7.addWidget(self.Label_4)

        self.TimeOfLastReportUpdatelabel_4 = QLabel(self.tab_5)
        self.TimeOfLastReportUpdatelabel_4.setObjectName(u"TimeOfLastReportUpdatelabel_4")
        self.TimeOfLastReportUpdatelabel_4.setFont(font)

        self.verticalLayout_7.addWidget(self.TimeOfLastReportUpdatelabel_4)

        self.gridLayout_14 = QGridLayout()
        self.gridLayout_14.setObjectName(u"gridLayout_14")
        self.OpenReportButton_4 = QPushButton(self.tab_5)
        self.OpenReportButton_4.setObjectName(u"OpenReportButton_4")
        self.OpenReportButton_4.setIcon(icon4)

        self.gridLayout_14.addWidget(self.OpenReportButton_4, 0, 0, 1, 1)

        self.ToReportDirButton_4 = QPushButton(self.tab_5)
        self.ToReportDirButton_4.setObjectName(u"ToReportDirButton_4")
        self.ToReportDirButton_4.setIcon(icon1)
        self.ToReportDirButton_4.setIconSize(QSize(18, 15))

        self.gridLayout_14.addWidget(self.ToReportDirButton_4, 0, 1, 1, 2)

        self.UpdateReportButton_4 = QPushButton(self.tab_5)
        self.UpdateReportButton_4.setObjectName(u"UpdateReportButton_4")
        self.UpdateReportButton_4.setIcon(icon2)
        self.UpdateReportButton_4.setIconSize(QSize(13, 13))

        self.gridLayout_14.addWidget(self.UpdateReportButton_4, 1, 0, 1, 2)

        self.ResetPriceReportButton_4 = QPushButton(self.tab_5)
        self.ResetPriceReportButton_4.setObjectName(u"ResetPriceReportButton_4")
        self.ResetPriceReportButton_4.setIcon(icon3)
        self.ResetPriceReportButton_4.setIconSize(QSize(15, 15))

        self.gridLayout_14.addWidget(self.ResetPriceReportButton_4, 1, 2, 1, 1)


        self.verticalLayout_7.addLayout(self.gridLayout_14)


        self.gridLayout_15.addLayout(self.verticalLayout_7, 1, 1, 1, 1)

        self.gridLayout_15.setColumnStretch(0, 7)
        self.gridLayout_15.setColumnStretch(1, 3)

        self.verticalLayout_9.addLayout(self.gridLayout_15)

        self.verticalLayout_9.setStretch(0, 3)
        self.verticalLayout_9.setStretch(1, 12)
        self.verticalLayout_9.setStretch(2, 35)

        self.gridLayout_16.addLayout(self.verticalLayout_9, 0, 0, 1, 1)

        self.tabWidget.addTab(self.tab_5, "")
        self.tab_3 = QWidget()
        self.tab_3.setObjectName(u"tab_3")
        self.gridLayout_13 = QGridLayout(self.tab_3)
        self.gridLayout_13.setObjectName(u"gridLayout_13")
        self.verticalLayout_5 = QVBoxLayout()
        self.verticalLayout_5.setObjectName(u"verticalLayout_5")
        self.horizontalLayout_11 = QHBoxLayout()
        self.horizontalLayout_11.setObjectName(u"horizontalLayout_11")
        self.StartButton_2 = QPushButton(self.tab_3)
        self.StartButton_2.setObjectName(u"StartButton_2")
        self.StartButton_2.setFont(font)
        self.StartButton_2.setStyleSheet(u"")
        self.StartButton_2.setIcon(icon)

        self.horizontalLayout_11.addWidget(self.StartButton_2)

        self.PauseCheckBox_2 = QCheckBox(self.tab_3)
        self.PauseCheckBox_2.setObjectName(u"PauseCheckBox_2")
        self.PauseCheckBox_2.setEnabled(False)

        self.horizontalLayout_11.addWidget(self.PauseCheckBox_2)

        self.horizontalSpacer_12 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_11.addItem(self.horizontalSpacer_12)

        self.ToReportDirButton_2 = QPushButton(self.tab_3)
        self.ToReportDirButton_2.setObjectName(u"ToReportDirButton_2")
        self.ToReportDirButton_2.setIcon(icon1)
        self.ToReportDirButton_2.setIconSize(QSize(18, 15))

        self.horizontalLayout_11.addWidget(self.ToReportDirButton_2)


        self.verticalLayout_5.addLayout(self.horizontalLayout_11)

        self.verticalSpacer_3 = QSpacerItem(20, 13, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Minimum)

        self.verticalLayout_5.addItem(self.verticalSpacer_3)

        self.gridLayout_6 = QGridLayout()
        self.gridLayout_6.setObjectName(u"gridLayout_6")
        self.horizontalSpacer_13 = QSpacerItem(20, 20, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Minimum)

        self.gridLayout_6.addItem(self.horizontalSpacer_13, 1, 0, 1, 1)

        self.horizontalSpacer_15 = QSpacerItem(20, 20, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Minimum)

        self.gridLayout_6.addItem(self.horizontalSpacer_15, 1, 4, 1, 1)

        self.horizontalLayout_15 = QHBoxLayout()
        self.horizontalLayout_15.setObjectName(u"horizontalLayout_15")
        self.CatalogUpdateTimeLabel_2 = QLabel(self.tab_3)
        self.CatalogUpdateTimeLabel_2.setObjectName(u"CatalogUpdateTimeLabel_2")
        self.CatalogUpdateTimeLabel_2.setFont(font1)
        self.CatalogUpdateTimeLabel_2.setAlignment(Qt.AlignmentFlag.AlignCenter)

        self.horizontalLayout_15.addWidget(self.CatalogUpdateTimeLabel_2)

        self.horizontalSpacer_22 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_15.addItem(self.horizontalSpacer_22)

        self.CatalogUpdateTimeTableUpdateButton_2 = QPushButton(self.tab_3)
        self.CatalogUpdateTimeTableUpdateButton_2.setObjectName(u"CatalogUpdateTimeTableUpdateButton_2")
        icon6 = QIcon(QIcon.fromTheme(QIcon.ThemeIcon.ViewRestore))
        self.CatalogUpdateTimeTableUpdateButton_2.setIcon(icon6)

        self.horizontalLayout_15.addWidget(self.CatalogUpdateTimeTableUpdateButton_2)


        self.gridLayout_6.addLayout(self.horizontalLayout_15, 0, 1, 1, 1)

        self.horizontalSpacer_14 = QSpacerItem(30, 20, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Minimum)

        self.gridLayout_6.addItem(self.horizontalSpacer_14, 1, 2, 1, 1)

        self.CurrencyTableView_2 = QTableView(self.tab_3)
        self.CurrencyTableView_2.setObjectName(u"CurrencyTableView_2")

        self.gridLayout_6.addWidget(self.CurrencyTableView_2, 1, 3, 1, 1)

        self.CatalogUpdateTimeTableView_2 = QTableView(self.tab_3)
        self.CatalogUpdateTimeTableView_2.setObjectName(u"CatalogUpdateTimeTableView_2")

        self.gridLayout_6.addWidget(self.CatalogUpdateTimeTableView_2, 1, 1, 1, 1)

        self.horizontalLayout_16 = QHBoxLayout()
        self.horizontalLayout_16.setObjectName(u"horizontalLayout_16")
        self.CurrencyLabel_2 = QLabel(self.tab_3)
        self.CurrencyLabel_2.setObjectName(u"CurrencyLabel_2")
        self.CurrencyLabel_2.setFont(font1)
        self.CurrencyLabel_2.setAlignment(Qt.AlignmentFlag.AlignCenter)

        self.horizontalLayout_16.addWidget(self.CurrencyLabel_2)

        self.horizontalSpacer_23 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_16.addItem(self.horizontalSpacer_23)

        self.CurrencyTableUpdateButton_2 = QPushButton(self.tab_3)
        self.CurrencyTableUpdateButton_2.setObjectName(u"CurrencyTableUpdateButton_2")
        self.CurrencyTableUpdateButton_2.setIcon(icon6)

        self.horizontalLayout_16.addWidget(self.CurrencyTableUpdateButton_2)


        self.gridLayout_6.addLayout(self.horizontalLayout_16, 0, 3, 1, 1)

        self.CatalogUpdateTime_TimeLabel2 = QLabel(self.tab_3)
        self.CatalogUpdateTime_TimeLabel2.setObjectName(u"CatalogUpdateTime_TimeLabel2")

        self.gridLayout_6.addWidget(self.CatalogUpdateTime_TimeLabel2, 2, 1, 1, 1)

        self.CurrencyTable_TimeLabel2 = QLabel(self.tab_3)
        self.CurrencyTable_TimeLabel2.setObjectName(u"CurrencyTable_TimeLabel2")

        self.gridLayout_6.addWidget(self.CurrencyTable_TimeLabel2, 2, 3, 1, 1)

        self.gridLayout_6.setColumnStretch(0, 3)
        self.gridLayout_6.setColumnStretch(1, 10)
        self.gridLayout_6.setColumnStretch(2, 3)
        self.gridLayout_6.setColumnStretch(3, 10)
        self.gridLayout_6.setColumnStretch(4, 3)

        self.verticalLayout_5.addLayout(self.gridLayout_6)

        self.verticalSpacer_6 = QSpacerItem(20, 13, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Minimum)

        self.verticalLayout_5.addItem(self.verticalSpacer_6)

        self.gridLayout_10 = QGridLayout()
        self.gridLayout_10.setObjectName(u"gridLayout_10")
        self.horizontalLayout_12 = QHBoxLayout()
        self.horizontalLayout_12.setObjectName(u"horizontalLayout_12")
        self.horizontalSpacer_16 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_12.addItem(self.horizontalSpacer_16)

        self.LogButton_2 = QPushButton(self.tab_3)
        self.LogButton_2.setObjectName(u"LogButton_2")
        self.LogButton_2.setIcon(icon5)

        self.horizontalLayout_12.addWidget(self.LogButton_2)


        self.gridLayout_10.addLayout(self.horizontalLayout_12, 0, 0, 1, 1)

        self.horizontalSpacer_19 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.gridLayout_10.addItem(self.horizontalSpacer_19, 0, 1, 1, 1)

        self.ConsoleTextBrowser_2 = QTextBrowser(self.tab_3)
        self.ConsoleTextBrowser_2.setObjectName(u"ConsoleTextBrowser_2")
        self.ConsoleTextBrowser_2.setSizeAdjustPolicy(QAbstractScrollArea.SizeAdjustPolicy.AdjustToContents)

        self.gridLayout_10.addWidget(self.ConsoleTextBrowser_2, 1, 0, 1, 1)

        self.verticalLayout_3 = QVBoxLayout()
        self.verticalLayout_3.setObjectName(u"verticalLayout_3")
        self.groupBox = QGroupBox(self.tab_3)
        self.groupBox.setObjectName(u"groupBox")
        self.gridLayout_8 = QGridLayout(self.groupBox)
        self.gridLayout_8.setObjectName(u"gridLayout_8")
        self.BasePriceTimeEdit_2 = QTimeEdit(self.groupBox)
        self.BasePriceTimeEdit_2.setObjectName(u"BasePriceTimeEdit_2")

        self.gridLayout_8.addWidget(self.BasePriceTimeEdit_2, 0, 1, 1, 1)

        self.CreateBasePriceButton_2 = QPushButton(self.groupBox)
        self.CreateBasePriceButton_2.setObjectName(u"CreateBasePriceButton_2")

        self.gridLayout_8.addWidget(self.CreateBasePriceButton_2, 0, 2, 1, 1)

        self.BasePriceLabel_2 = QLabel(self.groupBox)
        self.BasePriceLabel_2.setObjectName(u"BasePriceLabel_2")
        font2 = QFont()
        font2.setPointSize(10)
        self.BasePriceLabel_2.setFont(font2)

        self.gridLayout_8.addWidget(self.BasePriceLabel_2, 0, 0, 1, 1)

        self.CreateMassOffersButton_2 = QPushButton(self.groupBox)
        self.CreateMassOffersButton_2.setObjectName(u"CreateMassOffersButton_2")

        self.gridLayout_8.addWidget(self.CreateMassOffersButton_2, 1, 2, 1, 1)

        self.MassOffersTimeEdit_2 = QTimeEdit(self.groupBox)
        self.MassOffersTimeEdit_2.setObjectName(u"MassOffersTimeEdit_2")

        self.gridLayout_8.addWidget(self.MassOffersTimeEdit_2, 1, 1, 1, 1)

        self.MassOffersLabel_2 = QLabel(self.groupBox)
        self.MassOffersLabel_2.setObjectName(u"MassOffersLabel_2")
        self.MassOffersLabel_2.setFont(font2)

        self.gridLayout_8.addWidget(self.MassOffersLabel_2, 1, 0, 1, 1)

        self.OpenBasePriceButton_2 = QPushButton(self.groupBox)
        self.OpenBasePriceButton_2.setObjectName(u"OpenBasePriceButton_2")

        self.gridLayout_8.addWidget(self.OpenBasePriceButton_2, 0, 3, 1, 1)

        self.OpenMassOffersButton_2 = QPushButton(self.groupBox)
        self.OpenMassOffersButton_2.setObjectName(u"OpenMassOffersButton_2")

        self.gridLayout_8.addWidget(self.OpenMassOffersButton_2, 1, 3, 1, 1)


        self.verticalLayout_3.addWidget(self.groupBox)

        self.horizontalLayout_13 = QHBoxLayout()
        self.horizontalLayout_13.setObjectName(u"horizontalLayout_13")
        self.horizontalSpacer_17 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_13.addItem(self.horizontalSpacer_17)

        self.TimeSaveButton_2 = QPushButton(self.tab_3)
        self.TimeSaveButton_2.setObjectName(u"TimeSaveButton_2")
        icon7 = QIcon(QIcon.fromTheme(QIcon.ThemeIcon.DocumentSave))
        self.TimeSaveButton_2.setIcon(icon7)

        self.horizontalLayout_13.addWidget(self.TimeSaveButton_2)

        self.horizontalSpacer_18 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_13.addItem(self.horizontalSpacer_18)


        self.verticalLayout_3.addLayout(self.horizontalLayout_13)

        self.verticalSpacer_11 = QSpacerItem(20, 13, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Minimum)

        self.verticalLayout_3.addItem(self.verticalSpacer_11)

        self.groupBox_3 = QGroupBox(self.tab_3)
        self.groupBox_3.setObjectName(u"groupBox_3")
        self.gridLayout_9 = QGridLayout(self.groupBox_3)
        self.gridLayout_9.setObjectName(u"gridLayout_9")
        self.TgNotificationLabel_2 = QLabel(self.groupBox_3)
        self.TgNotificationLabel_2.setObjectName(u"TgNotificationLabel_2")
        self.TgNotificationLabel_2.setFont(font2)

        self.gridLayout_9.addWidget(self.TgNotificationLabel_2, 0, 0, 1, 1)

        self.Tg_timeEdit_2 = QTimeEdit(self.groupBox_3)
        self.Tg_timeEdit_2.setObjectName(u"Tg_timeEdit_2")

        self.gridLayout_9.addWidget(self.Tg_timeEdit_2, 0, 1, 1, 1)

        self.TgNTimeSaveButton_2 = QPushButton(self.groupBox_3)
        self.TgNTimeSaveButton_2.setObjectName(u"TgNTimeSaveButton_2")

        self.gridLayout_9.addWidget(self.TgNTimeSaveButton_2, 0, 2, 1, 1)

        self.Update3_0_ConditionLabel_2 = QLabel(self.groupBox_3)
        self.Update3_0_ConditionLabel_2.setObjectName(u"Update3_0_ConditionLabel_2")
        self.Update3_0_ConditionLabel_2.setFont(font2)

        self.gridLayout_9.addWidget(self.Update3_0_ConditionLabel_2, 1, 0, 1, 1)

        self.Update3_0_Condition_timeEdit_2 = QTimeEdit(self.groupBox_3)
        self.Update3_0_Condition_timeEdit_2.setObjectName(u"Update3_0_Condition_timeEdit_2")

        self.gridLayout_9.addWidget(self.Update3_0_Condition_timeEdit_2, 1, 1, 1, 1)

        self.Update3_0_ConditionSaveButton_2 = QPushButton(self.groupBox_3)
        self.Update3_0_ConditionSaveButton_2.setObjectName(u"Update3_0_ConditionSaveButton_2")

        self.gridLayout_9.addWidget(self.Update3_0_ConditionSaveButton_2, 1, 2, 1, 1)


        self.verticalLayout_3.addWidget(self.groupBox_3)

        self.verticalSpacer_4 = QSpacerItem(20, 13, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Minimum)

        self.verticalLayout_3.addItem(self.verticalSpacer_4)

        self.groupBox_2 = QGroupBox(self.tab_3)
        self.groupBox_2.setObjectName(u"groupBox_2")
        self.gridLayout_7 = QGridLayout(self.groupBox_2)
        self.gridLayout_7.setObjectName(u"gridLayout_7")
        self.ResetMailReporButton_2 = QPushButton(self.groupBox_2)
        self.ResetMailReporButton_2.setObjectName(u"ResetMailReporButton_2")

        self.gridLayout_7.addWidget(self.ResetMailReporButton_2, 2, 2, 1, 1)

        self.OpenPriceStatusButton_2 = QPushButton(self.groupBox_2)
        self.OpenPriceStatusButton_2.setObjectName(u"OpenPriceStatusButton_2")

        self.gridLayout_7.addWidget(self.OpenPriceStatusButton_2, 1, 3, 1, 1)

        self.CreatePriceStatusButton_2 = QPushButton(self.groupBox_2)
        self.CreatePriceStatusButton_2.setObjectName(u"CreatePriceStatusButton_2")

        self.gridLayout_7.addWidget(self.CreatePriceStatusButton_2, 1, 1, 1, 1)

        self.PriceStatusLabel_2 = QLabel(self.groupBox_2)
        self.PriceStatusLabel_2.setObjectName(u"PriceStatusLabel_2")
        self.PriceStatusLabel_2.setFont(font2)

        self.gridLayout_7.addWidget(self.PriceStatusLabel_2, 1, 0, 1, 1)

        self.CreateMailReporButton_2 = QPushButton(self.groupBox_2)
        self.CreateMailReporButton_2.setObjectName(u"CreateMailReporButton_2")

        self.gridLayout_7.addWidget(self.CreateMailReporButton_2, 2, 1, 1, 1)

        self.OpenMailReporButton_2 = QPushButton(self.groupBox_2)
        self.OpenMailReporButton_2.setObjectName(u"OpenMailReporButton_2")

        self.gridLayout_7.addWidget(self.OpenMailReporButton_2, 2, 3, 1, 1)

        self.ResetPriceStatusButton_2 = QPushButton(self.groupBox_2)
        self.ResetPriceStatusButton_2.setObjectName(u"ResetPriceStatusButton_2")

        self.gridLayout_7.addWidget(self.ResetPriceStatusButton_2, 1, 2, 1, 1)

        self.MailReportLabel_2 = QLabel(self.groupBox_2)
        self.MailReportLabel_2.setObjectName(u"MailReportLabel_2")
        self.MailReportLabel_2.setFont(font2)

        self.gridLayout_7.addWidget(self.MailReportLabel_2, 2, 0, 1, 1)


        self.verticalLayout_3.addWidget(self.groupBox_2)

        self.verticalSpacer_10 = QSpacerItem(20, 17, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Minimum)

        self.verticalLayout_3.addItem(self.verticalSpacer_10)

        self.horizontalLayout_23 = QHBoxLayout()
        self.horizontalLayout_23.setObjectName(u"horizontalLayout_23")
        self.horizontalSpacer_24 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_23.addItem(self.horizontalSpacer_24)

        self.CreateTotalCsv_2 = QPushButton(self.tab_3)
        self.CreateTotalCsv_2.setObjectName(u"CreateTotalCsv_2")

        self.horizontalLayout_23.addWidget(self.CreateTotalCsv_2)

        self.horizontalSpacer_31 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_23.addItem(self.horizontalSpacer_31)


        self.verticalLayout_3.addLayout(self.horizontalLayout_23)

        self.TotalCountLabel_2 = QLabel(self.tab_3)
        self.TotalCountLabel_2.setObjectName(u"TotalCountLabel_2")
        self.TotalCountLabel_2.setFont(font)
        self.TotalCountLabel_2.setAlignment(Qt.AlignmentFlag.AlignCenter)

        self.verticalLayout_3.addWidget(self.TotalCountLabel_2)

        self.verticalSpacer_7 = QSpacerItem(20, 17, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Minimum)

        self.verticalLayout_3.addItem(self.verticalSpacer_7)

        self.horizontalLayout_14 = QHBoxLayout()
        self.horizontalLayout_14.setObjectName(u"horizontalLayout_14")
        self.horizontalSpacer_20 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_14.addItem(self.horizontalSpacer_20)

        self.ResetDBButton_2 = QPushButton(self.tab_3)
        self.ResetDBButton_2.setObjectName(u"ResetDBButton_2")

        self.horizontalLayout_14.addWidget(self.ResetDBButton_2)

        self.horizontalSpacer_21 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_14.addItem(self.horizontalSpacer_21)


        self.verticalLayout_3.addLayout(self.horizontalLayout_14)

        self.verticalSpacer_5 = QSpacerItem(20, 40, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Expanding)

        self.verticalLayout_3.addItem(self.verticalSpacer_5)


        self.gridLayout_10.addLayout(self.verticalLayout_3, 1, 1, 1, 1)

        self.gridLayout_10.setColumnStretch(0, 7)
        self.gridLayout_10.setColumnStretch(1, 3)

        self.verticalLayout_5.addLayout(self.gridLayout_10)

        self.verticalLayout_5.setStretch(0, 3)
        self.verticalLayout_5.setStretch(1, 1)
        self.verticalLayout_5.setStretch(2, 10)
        self.verticalLayout_5.setStretch(3, 1)
        self.verticalLayout_5.setStretch(4, 25)

        self.gridLayout_13.addLayout(self.verticalLayout_5, 0, 0, 1, 1)

        self.tabWidget.addTab(self.tab_3, "")

        self.gridLayout.addWidget(self.tabWidget, 0, 0, 1, 1)

        MainWindow.setCentralWidget(self.centralwidget)
        self.menubar = QMenuBar(MainWindow)
        self.menubar.setObjectName(u"menubar")
        self.menubar.setGeometry(QRect(0, 0, 1113, 22))
        MainWindow.setMenuBar(self.menubar)
        self.statusbar = QStatusBar(MainWindow)
        self.statusbar.setObjectName(u"statusbar")
        MainWindow.setStatusBar(self.statusbar)

        self.retranslateUi(MainWindow)

        self.tabWidget.setCurrentIndex(4)


        QMetaObject.connectSlotsByName(MainWindow)
    # setupUi

    def retranslateUi(self, MainWindow):
        MainWindow.setWindowTitle(QCoreApplication.translate("MainWindow", u"MainWindow", None))
        self.StartButton_0.setText(QCoreApplication.translate("MainWindow", u"\u0417\u0430\u043f\u0443\u0441\u043a", None))
        self.PauseCheckBox_0.setText(QCoreApplication.translate("MainWindow", u"\u041f\u0430\u0443\u0437\u0430", None))
        self.ToMailFilesDirButton_0.setText(QCoreApplication.translate("MainWindow", u"\u041f\u0435\u0440\u0435\u0439\u0442\u0438 \u0432 \u043f\u0430\u043f\u043a\u0443 \u0441 \u043f\u0440\u0430\u0439\u0441\u0430\u043c\u0438", None))
        self.TableLabel_0.setText(QCoreApplication.translate("MainWindow", u"\u0422\u0430\u0431\u043b\u0438\u0446\u0430 \u0441 \u043d\u0435\u0441\u043e\u0445\u0440\u0430\u043d\u0451\u043d\u043d\u044b\u043c\u0438 \u043f\u0440\u0430\u0439\u0441\u0430\u043c\u0438:", None))
        self.UpdateReportButton_0.setText(QCoreApplication.translate("MainWindow", u"\u041e\u0431\u043d\u043e\u0432\u0438\u0442\u044c \u043e\u0442\u0447\u0451\u0442", None))
        self.ResetMailReportUnloadedButton_0.setText(QCoreApplication.translate("MainWindow", u"\u041e\u0431\u043d\u0443\u043b\u0438\u0442\u044c \u043e\u0442\u0447\u0451\u0442", None))
        self.ResetMailReportButton_0.setText(QCoreApplication.translate("MainWindow", u"\u041e\u0431\u043d\u0443\u043b\u0438\u0442\u044c \u0438\u043d\u0444\u043e\u0440\u043c\u0430\u0446\u0438\u044e \u043e \u043f\u0438\u0441\u044c\u043c\u0430\u0445 \u0432 \u0411\u0414", None))
        self.OpenReportButton_0.setText(QCoreApplication.translate("MainWindow", u"\u041e\u0442\u043a\u0440\u044b\u0442\u044c \u043e\u0442\u0447\u0451\u0442", None))
        self.ToMailReportDirButton_0.setText(QCoreApplication.translate("MainWindow", u"\u041f\u0435\u0440\u0435\u0439\u0442\u0438 \u0432 \u043f\u0430\u043f\u043a\u0443 \u0441 \u043e\u0442\u0447\u0451\u0442\u043e\u043c", None))
        self.Label_0.setText(QCoreApplication.translate("MainWindow", u"\u0412\u0440\u0435\u043c\u044f \u043f\u043e\u0441\u043b\u0435\u0434\u043d\u0435\u0433\u043e \u043e\u0431\u043d\u043e\u0432\u043b\u0435\u043d\u0438\u044f \u043e\u0442\u0447\u0451\u0442\u0430:", None))
        self.TimeOfLastReportUpdatelabel_0.setText(QCoreApplication.translate("MainWindow", u"-", None))
#if QT_CONFIG(tooltip)
        self.LogButton_0.setToolTip("")
#endif // QT_CONFIG(tooltip)
#if QT_CONFIG(whatsthis)
        self.LogButton_0.setWhatsThis("")
#endif // QT_CONFIG(whatsthis)
        self.LogButton_0.setText("")
#if QT_CONFIG(shortcut)
        self.LogButton_0.setShortcut("")
#endif // QT_CONFIG(shortcut)
#if QT_CONFIG(tooltip)
        self.ConsoleTextBrowser_0.setToolTip("")
#endif // QT_CONFIG(tooltip)
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.tab_2), QCoreApplication.translate("MainWindow", u"\u041f\u043e\u0447\u0442\u0430", None))
        self.StartButton_1.setText(QCoreApplication.translate("MainWindow", u"\u0417\u0430\u043f\u0443\u0441\u043a", None))
        self.PauseCheckBox_1.setText(QCoreApplication.translate("MainWindow", u"\u041f\u0430\u0443\u0437\u0430", None))
        self.label.setText(QCoreApplication.translate("MainWindow", u"\u041c\u0438\u043d. \u0440\u0430\u0437\u043c\u0435\u0440 \u0444\u0430\u0439\u043b\u0430 \u0434\u043b\u044f \u043e\u0431\u0440\u0430\u0431\u043e\u0442\u043a\u0435 \u0432 \u0434\u043e\u043f. \u043f\u043e\u0442\u043e\u043a\u0435 (\u041c\u0411):", None))
        self.MB_SaveButton_1.setText(QCoreApplication.translate("MainWindow", u"\u0421\u043e\u0445\u0440\u0430\u043d\u0438\u0442\u044c", None))
        self.ToFilesDirButton_1.setText(QCoreApplication.translate("MainWindow", u"\u041f\u0435\u0440\u0435\u0439\u0442\u0438 \u0432 \u043f\u0430\u043f\u043a\u0443 \u0441 \u043f\u0440\u0430\u0439\u0441\u0430\u043c\u0438", None))
        self.LogButton_1.setText("")
        self.NotMatchedLabel_1.setText(QCoreApplication.translate("MainWindow", u"\u0421\u0442\u0430\u0442\u0443\u0441 \u043f\u0440\u0430\u0439\u0441\u043e\u0432:", None))
        self.Label_1.setText(QCoreApplication.translate("MainWindow", u"\u0412\u0440\u0435\u043c\u044f \u043f\u043e\u0441\u043b\u0435\u0434\u043d\u0435\u0433\u043e \u043e\u0431\u043d\u043e\u0432\u043b\u0435\u043d\u0438\u044f \u043e\u0442\u0447\u0451\u0442\u0430:", None))
        self.TimeOfLastReportUpdatelabel_1.setText(QCoreApplication.translate("MainWindow", u"-", None))
        self.OpenReportButton_1.setText(QCoreApplication.translate("MainWindow", u"\u041e\u0442\u043a\u0440\u044b\u0442\u044c \u043e\u0442\u0447\u0451\u0442", None))
        self.ToReportDirButton_1.setText(QCoreApplication.translate("MainWindow", u"\u041f\u0435\u0440\u0435\u0439\u0442\u0438 \u0432 \u043f\u0430\u043f\u043a\u0443 \u0441 \u043e\u0442\u0447\u0451\u0442\u043e\u043c", None))
        self.UpdateReportButton_1.setText(QCoreApplication.translate("MainWindow", u"\u041e\u0431\u043d\u043e\u0432\u0438\u0442\u044c \u043e\u0442\u0447\u0451\u0442", None))
        self.ResetPriceReportButton_1.setText(QCoreApplication.translate("MainWindow", u"\u041e\u0431\u043d\u0443\u043b\u0438\u0442\u044c \u043e\u0442\u0447\u0451\u0442", None))
        self.progressBar_1.setFormat(QCoreApplication.translate("MainWindow", u"%p%", None))
        self.ProgressLabel_1.setText(QCoreApplication.translate("MainWindow", u"0/0", None))
        self.TotalTimeLabel.setText(QCoreApplication.translate("MainWindow", u"[0:00:00]", None))
        self.progressBar_1_1.setFormat(QCoreApplication.translate("MainWindow", u"%p%", None))
        self.ProgressLabel_1_1.setText(QCoreApplication.translate("MainWindow", u"0/0", None))
        self.TotalTimeLabel_1_1.setText(QCoreApplication.translate("MainWindow", u"[0:00:00]", None))
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.tab), QCoreApplication.translate("MainWindow", u"\u0421\u0442\u0430\u043d\u0434\u0430\u0440\u0442\u0438\u0437\u0430\u0446\u0438\u044f", None))
        self.StartButton_3.setText(QCoreApplication.translate("MainWindow", u"\u0417\u0430\u043f\u0443\u0441\u043a", None))
        self.PauseCheckBox_3.setText(QCoreApplication.translate("MainWindow", u"\u041f\u0430\u0443\u0437\u0430", None))
        self.label_2.setText(QCoreApplication.translate("MainWindow", u"\u041c\u0438\u043d. \u0440\u0430\u0437\u043c\u0435\u0440 \u0444\u0430\u0439\u043b\u0430 \u0434\u043b\u044f \u043e\u0431\u0440\u0430\u0431\u043e\u0442\u043a\u0435 \u0432 \u0434\u043e\u043f. \u043f\u043e\u0442\u043e\u043a\u0435 (\u041c\u0411):", None))
        self.MB_SaveButton_2.setText(QCoreApplication.translate("MainWindow", u"\u0421\u043e\u0445\u0440\u0430\u043d\u0438\u0442\u044c", None))
        self.ToFilesDirButton_3.setText(QCoreApplication.translate("MainWindow", u"\u041f\u0435\u0440\u0435\u0439\u0442\u0438 \u0432 \u043f\u0430\u043f\u043a\u0443 \u0441 \u043f\u0440\u0430\u0439\u0441\u0430\u043c\u0438", None))
        self.LogButton_3.setText("")
        self.Label_3.setText(QCoreApplication.translate("MainWindow", u"\u0412\u0440\u0435\u043c\u044f \u043f\u043e\u0441\u043b\u0435\u0434\u043d\u0435\u0433\u043e \u043e\u0431\u043d\u043e\u0432\u043b\u0435\u043d\u0438\u044f \u043e\u0442\u0447\u0451\u0442\u0430:", None))
        self.TimeOfLastReportUpdatelabel_3.setText(QCoreApplication.translate("MainWindow", u"-", None))
        self.OpenReportButton_3.setText(QCoreApplication.translate("MainWindow", u"\u041e\u0442\u043a\u0440\u044b\u0442\u044c \u043e\u0442\u0447\u0451\u0442", None))
        self.ToReportDirButton_3.setText(QCoreApplication.translate("MainWindow", u"\u041f\u0435\u0440\u0435\u0439\u0442\u0438 \u0432 \u043f\u0430\u043f\u043a\u0443 \u0441 \u043e\u0442\u0447\u0451\u0442\u043e\u043c", None))
        self.UpdateReportButton_3.setText(QCoreApplication.translate("MainWindow", u"\u041e\u0431\u043d\u043e\u0432\u0438\u0442\u044c \u043e\u0442\u0447\u0451\u0442", None))
        self.ResetPriceReportButton_3.setText(QCoreApplication.translate("MainWindow", u"\u041e\u0431\u043d\u0443\u043b\u0438\u0442\u044c \u043e\u0442\u0447\u0451\u0442", None))
        self.progressBar_3.setFormat(QCoreApplication.translate("MainWindow", u"%p%", None))
        self.ProgressLabel_3.setText(QCoreApplication.translate("MainWindow", u"0/0", None))
        self.TotalTimeLabel_3.setText(QCoreApplication.translate("MainWindow", u"[0:00:00]", None))
        self.progressBar_3_1.setFormat(QCoreApplication.translate("MainWindow", u"%p%", None))
        self.ProgressLabel_3_1.setText(QCoreApplication.translate("MainWindow", u"0/0", None))
        self.TotalTimeLabel_3_1.setText(QCoreApplication.translate("MainWindow", u"[0:00:00]", None))
        self.NotMatchedLabel_3.setText(QCoreApplication.translate("MainWindow", u"\u0421\u0442\u0430\u0442\u0443\u0441 \u043f\u0440\u0430\u0439\u0441\u043e\u0432:", None))
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.tab_4), QCoreApplication.translate("MainWindow", u"\u041e\u0431\u0440\u0430\u0431\u043e\u0442\u043a\u0430", None))
        self.StartButton_4.setText(QCoreApplication.translate("MainWindow", u"\u0417\u0430\u043f\u0443\u0441\u043a", None))
        self.PauseCheckBox_4.setText(QCoreApplication.translate("MainWindow", u"\u041f\u0430\u0443\u0437\u0430", None))
        self.SendCheckBox_4.setText(QCoreApplication.translate("MainWindow", u"\u0424\u043e\u0440\u043c\u0438\u0440\u043e\u0432\u0430\u0442\u044c, \u043d\u043e \u043d\u0435 \u043e\u0442\u043f\u0440\u0430\u0432\u043b\u044f\u0442\u044c", None))
        self.ToFilesDirButton_4.setText(QCoreApplication.translate("MainWindow", u"\u041f\u0435\u0440\u0435\u0439\u0442\u0438 \u0432 \u043f\u0430\u043f\u043a\u0443 \u0441 \u043f\u0440\u0430\u0439\u0441\u0430\u043c\u0438", None))
        self.LogButton_4.setText("")
        self.NotMatchedLabel_4.setText(QCoreApplication.translate("MainWindow", u"\u0421\u0442\u0430\u0442\u0443\u0441 \u043f\u0440\u0430\u0439\u0441\u043e\u0432:", None))
        self.progressBar_4.setFormat(QCoreApplication.translate("MainWindow", u"%p%", None))
        self.ProgressLabel_4.setText(QCoreApplication.translate("MainWindow", u"0/0", None))
        self.TotalTimeLabel_4.setText(QCoreApplication.translate("MainWindow", u"[0:00:00]", None))
        self.Label_4.setText(QCoreApplication.translate("MainWindow", u"\u0412\u0440\u0435\u043c\u044f \u043f\u043e\u0441\u043b\u0435\u0434\u043d\u0435\u0433\u043e \u043e\u0431\u043d\u043e\u0432\u043b\u0435\u043d\u0438\u044f \u043e\u0442\u0447\u0451\u0442\u0430:", None))
        self.TimeOfLastReportUpdatelabel_4.setText(QCoreApplication.translate("MainWindow", u"-", None))
        self.OpenReportButton_4.setText(QCoreApplication.translate("MainWindow", u"\u041e\u0442\u043a\u0440\u044b\u0442\u044c \u043e\u0442\u0447\u0451\u0442", None))
        self.ToReportDirButton_4.setText(QCoreApplication.translate("MainWindow", u"\u041f\u0435\u0440\u0435\u0439\u0442\u0438 \u0432 \u043f\u0430\u043f\u043a\u0443 \u0441 \u043e\u0442\u0447\u0451\u0442\u043e\u043c", None))
        self.UpdateReportButton_4.setText(QCoreApplication.translate("MainWindow", u"\u041e\u0431\u043d\u043e\u0432\u0438\u0442\u044c \u043e\u0442\u0447\u0451\u0442", None))
        self.ResetPriceReportButton_4.setText(QCoreApplication.translate("MainWindow", u"\u041e\u0431\u043d\u0443\u043b\u0438\u0442\u044c \u043e\u0442\u0447\u0451\u0442", None))
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.tab_5), QCoreApplication.translate("MainWindow", u"\u0420\u0430\u0441\u0441\u044b\u043b\u043a\u0430", None))
        self.StartButton_2.setText(QCoreApplication.translate("MainWindow", u"\u0417\u0430\u043f\u0443\u0441\u043a", None))
        self.PauseCheckBox_2.setText(QCoreApplication.translate("MainWindow", u"\u041f\u0430\u0443\u0437\u0430", None))
        self.ToReportDirButton_2.setText(QCoreApplication.translate("MainWindow", u"\u041f\u0435\u0440\u0435\u0439\u0442\u0438 \u0432 \u043f\u0430\u043f\u043a\u0443 \u0441\u043e \u0441\u043f\u0440\u0430\u0432\u043e\u0447\u043d\u0438\u043a\u0430\u043c\u0438 \u0438 \u043e\u0442\u0447\u0451\u0442\u0430\u043c\u0438", None))
        self.CatalogUpdateTimeLabel_2.setText(QCoreApplication.translate("MainWindow", u"\u041f\u043e\u0441\u043b\u0435\u0434\u043d\u0438\u0435 \u043e\u0431\u043d\u043e\u0432\u043b\u0435\u043d\u0438\u044f - \u0441\u043f\u0440\u0430\u0432\u043e\u0447\u043d\u0438\u043a\u0438:", None))
        self.CatalogUpdateTimeTableUpdateButton_2.setText("")
        self.CurrencyLabel_2.setText(QCoreApplication.translate("MainWindow", u"\u041a\u0443\u0440\u0441 \u0432\u0430\u043b\u044e\u0442:", None))
        self.CurrencyTableUpdateButton_2.setText("")
        self.CatalogUpdateTime_TimeLabel2.setText(QCoreApplication.translate("MainWindow", u"-", None))
        self.CurrencyTable_TimeLabel2.setText(QCoreApplication.translate("MainWindow", u"-", None))
        self.LogButton_2.setText("")
        self.groupBox.setTitle("")
        self.CreateBasePriceButton_2.setText(QCoreApplication.translate("MainWindow", u"\u0421\u0444\u043e\u0440\u043c\u0438\u0440\u043e\u0432\u0430\u0442\u044c", None))
        self.BasePriceLabel_2.setText(QCoreApplication.translate("MainWindow", u"\u0411\u0430\u0437\u043e\u0432\u0430\u044f \u0446\u0435\u043d\u0430", None))
        self.CreateMassOffersButton_2.setText(QCoreApplication.translate("MainWindow", u"\u0421\u0444\u043e\u0440\u043c\u0438\u0440\u043e\u0432\u0430\u0442\u044c", None))
        self.MassOffersLabel_2.setText(QCoreApplication.translate("MainWindow", u"\u041f\u0440\u0435\u0434\u043b\u043e\u0436\u0435\u043d\u0438\u0439 \u0432 \u043e\u043f\u0442\u0435", None))
        self.OpenBasePriceButton_2.setText(QCoreApplication.translate("MainWindow", u"\u041e\u0442\u043a\u0440\u044b\u0442\u044c", None))
        self.OpenMassOffersButton_2.setText(QCoreApplication.translate("MainWindow", u"\u041e\u0442\u043a\u0440\u044b\u0442\u044c", None))
        self.TimeSaveButton_2.setText(QCoreApplication.translate("MainWindow", u"\u0421\u043e\u0445\u0440\u0430\u043d\u0438\u0442\u044c \u0432\u0440\u0435\u043c\u044f", None))
        self.groupBox_3.setTitle("")
        self.TgNotificationLabel_2.setText(QCoreApplication.translate("MainWindow", u"\u0412\u0440\u0435\u043c\u044f \u043e\u0442\u043f\u0440\u0430\u0432\u043a\u0438 \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f \u0432 Telegram", None))
        self.TgNTimeSaveButton_2.setText(QCoreApplication.translate("MainWindow", u"\u0421\u043e\u0445\u0440\u0430\u043d\u0438\u0442\u044c", None))
        self.Update3_0_ConditionLabel_2.setText(QCoreApplication.translate("MainWindow", u"\u0412\u0440\u0435\u043c\u044f \u043f\u043e\u043b\u043d\u043e\u0433\u043e \u043e\u0431\u043d\u043e\u0432\u043b\u0435\u043d\u0438\u044f \u043f\u043e 3.0 \u0423\u0441\u043b\u043e\u0432\u0438\u044f\u043c", None))
        self.Update3_0_ConditionSaveButton_2.setText(QCoreApplication.translate("MainWindow", u"\u0421\u043e\u0445\u0440\u0430\u043d\u0438\u0442\u044c", None))
        self.groupBox_2.setTitle("")
        self.ResetMailReporButton_2.setText(QCoreApplication.translate("MainWindow", u"\u041e\u0431\u043d\u0443\u043b\u0438\u0442\u044c \u043e\u0442\u0447\u0451\u0442", None))
        self.OpenPriceStatusButton_2.setText(QCoreApplication.translate("MainWindow", u"\u041e\u0442\u043a\u0440\u044b\u0442\u044c", None))
        self.CreatePriceStatusButton_2.setText(QCoreApplication.translate("MainWindow", u"\u0421\u043e\u0437\u0434\u0430\u0442\u044c \u043e\u0442\u0447\u0451\u0442", None))
        self.PriceStatusLabel_2.setText(QCoreApplication.translate("MainWindow", u"\u0421\u043e\u0441\u0442\u043e\u044f\u043d\u0438\u0435 \u043f\u0440\u0430\u0439\u0441\u043e\u0432", None))
        self.CreateMailReporButton_2.setText(QCoreApplication.translate("MainWindow", u"\u0421\u043e\u0437\u0434\u0430\u0442\u044c \u043e\u0442\u0447\u0451\u0442", None))
        self.OpenMailReporButton_2.setText(QCoreApplication.translate("MainWindow", u"\u041e\u0442\u043a\u0440\u044b\u0442\u044c", None))
        self.ResetPriceStatusButton_2.setText(QCoreApplication.translate("MainWindow", u"\u041e\u0431\u043d\u0443\u043b\u0438\u0442\u044c \u043e\u0442\u0447\u0451\u0442", None))
        self.MailReportLabel_2.setText(QCoreApplication.translate("MainWindow", u"\u041e\u0442\u0447\u0451\u0442 \u043f\u043e\u0447\u0442\u0430", None))
        self.CreateTotalCsv_2.setText(QCoreApplication.translate("MainWindow", u"\u0421\u043e\u0437\u0434\u0430\u0442\u044c \u0438\u0442\u043e\u0433\u043e\u0432\u044b\u0439 \u043f\u0440\u0430\u0439\u0441", None))
        self.TotalCountLabel_2.setText(QCoreApplication.translate("MainWindow", u"\u0412\u0441\u0435\u0433\u043e \u043f\u043e\u0437\u0438\u0446\u0438\u0439: -", None))
        self.ResetDBButton_2.setText(QCoreApplication.translate("MainWindow", u"\u041e\u0431\u043d\u0443\u043b\u0438\u0442\u044c \u0411\u0414", None))
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.tab_3), QCoreApplication.translate("MainWindow", u"\u0421\u043f\u0440\u0430\u0432\u043e\u0447\u043d\u0438\u043a\u0438", None))
    # retranslateUi

