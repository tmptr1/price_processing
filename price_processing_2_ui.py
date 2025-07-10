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
    QGridLayout, QHBoxLayout, QHeaderView, QLabel,
    QMainWindow, QMenuBar, QProgressBar, QPushButton,
    QSizePolicy, QSpacerItem, QSpinBox, QStatusBar,
    QTabWidget, QTableView, QTextBrowser, QVBoxLayout,
    QWidget)

class Ui_MainWindow(object):
    def setupUi(self, MainWindow):
        if not MainWindow.objectName():
            MainWindow.setObjectName(u"MainWindow")
        MainWindow.resize(767, 681)
        self.centralwidget = QWidget(MainWindow)
        self.centralwidget.setObjectName(u"centralwidget")
        self.gridLayout = QGridLayout(self.centralwidget)
        self.gridLayout.setObjectName(u"gridLayout")
        self.tabWidget = QTabWidget(self.centralwidget)
        self.tabWidget.setObjectName(u"tabWidget")
        self.tab = QWidget()
        self.tab.setObjectName(u"tab")
        self.gridLayout_2 = QGridLayout(self.tab)
        self.gridLayout_2.setObjectName(u"gridLayout_2")
        self.verticalLayout_3 = QVBoxLayout()
        self.verticalLayout_3.setObjectName(u"verticalLayout_3")
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


        self.verticalLayout_3.addLayout(self.horizontalLayout)

        self.verticalSpacer = QSpacerItem(20, 20, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Minimum)

        self.verticalLayout_3.addItem(self.verticalSpacer)

        self.verticalLayout_2 = QVBoxLayout()
        self.verticalLayout_2.setObjectName(u"verticalLayout_2")
        self.verticalLayout = QVBoxLayout()
        self.verticalLayout.setObjectName(u"verticalLayout")
        self.horizontalLayout_2 = QHBoxLayout()
        self.horizontalLayout_2.setObjectName(u"horizontalLayout_2")
        self.StartButton_1 = QPushButton(self.tab)
        self.StartButton_1.setObjectName(u"StartButton_1")

        self.horizontalLayout_2.addWidget(self.StartButton_1)

        self.pushButton_2 = QPushButton(self.tab)
        self.pushButton_2.setObjectName(u"pushButton_2")

        self.horizontalLayout_2.addWidget(self.pushButton_2)

        self.ThreadLabel_1 = QLabel(self.tab)
        self.ThreadLabel_1.setObjectName(u"ThreadLabel_1")
        font = QFont()
        font.setPointSize(10)
        self.ThreadLabel_1.setFont(font)

        self.horizontalLayout_2.addWidget(self.ThreadLabel_1)

        self.ThreadSpinBox = QSpinBox(self.tab)
        self.ThreadSpinBox.setObjectName(u"ThreadSpinBox")
        self.ThreadSpinBox.setMinimum(1)
        self.ThreadSpinBox.setMaximum(10)

        self.horizontalLayout_2.addWidget(self.ThreadSpinBox)

        self.horizontalSpacer_3 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_2.addItem(self.horizontalSpacer_3)


        self.verticalLayout.addLayout(self.horizontalLayout_2)

        self.PauseCheckBox_1 = QCheckBox(self.tab)
        self.PauseCheckBox_1.setObjectName(u"PauseCheckBox_1")
        self.PauseCheckBox_1.setEnabled(False)

        self.verticalLayout.addWidget(self.PauseCheckBox_1)


        self.verticalLayout_2.addLayout(self.verticalLayout)

        self.ConsoleTextBrowser_1 = QTextBrowser(self.tab)
        self.ConsoleTextBrowser_1.setObjectName(u"ConsoleTextBrowser_1")
        self.ConsoleTextBrowser_1.setSizeAdjustPolicy(QAbstractScrollArea.SizeAdjustPolicy.AdjustToContents)

        self.verticalLayout_2.addWidget(self.ConsoleTextBrowser_1)

        self.horizontalLayout_3 = QHBoxLayout()
        self.horizontalLayout_3.setObjectName(u"horizontalLayout_3")
        self.progressBar_1 = QProgressBar(self.tab)
        self.progressBar_1.setObjectName(u"progressBar_1")
        self.progressBar_1.setMaximumSize(QSize(16777215, 14))
        self.progressBar_1.setValue(0)
        self.progressBar_1.setTextVisible(False)

        self.horizontalLayout_3.addWidget(self.progressBar_1)

        self.ProgressLabel_1 = QLabel(self.tab)
        self.ProgressLabel_1.setObjectName(u"ProgressLabel_1")

        self.horizontalLayout_3.addWidget(self.ProgressLabel_1)

        self.horizontalSpacer_4 = QSpacerItem(40, 20, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_3.addItem(self.horizontalSpacer_4)

        self.TotalTimeLabel = QLabel(self.tab)
        self.TotalTimeLabel.setObjectName(u"TotalTimeLabel")
        self.TotalTimeLabel.setFrameShape(QFrame.Shape.NoFrame)

        self.horizontalLayout_3.addWidget(self.TotalTimeLabel)

        self.horizontalSpacer_5 = QSpacerItem(40, 20, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_3.addItem(self.horizontalSpacer_5)


        self.verticalLayout_2.addLayout(self.horizontalLayout_3)


        self.verticalLayout_3.addLayout(self.verticalLayout_2)

        self.verticalLayout_3.setStretch(0, 20)
        self.verticalLayout_3.setStretch(1, 5)
        self.verticalLayout_3.setStretch(2, 75)

        self.gridLayout_2.addLayout(self.verticalLayout_3, 0, 0, 1, 1)

        self.tabWidget.addTab(self.tab, "")
        self.tab_2 = QWidget()
        self.tab_2.setObjectName(u"tab_2")
        self.tabWidget.addTab(self.tab_2, "")

        self.gridLayout.addWidget(self.tabWidget, 0, 0, 1, 1)

        MainWindow.setCentralWidget(self.centralwidget)
        self.menubar = QMenuBar(MainWindow)
        self.menubar.setObjectName(u"menubar")
        self.menubar.setGeometry(QRect(0, 0, 767, 22))
        MainWindow.setMenuBar(self.menubar)
        self.statusbar = QStatusBar(MainWindow)
        self.statusbar.setObjectName(u"statusbar")
        MainWindow.setStatusBar(self.statusbar)

        self.retranslateUi(MainWindow)

        self.tabWidget.setCurrentIndex(0)


        QMetaObject.connectSlotsByName(MainWindow)
    # setupUi

    def retranslateUi(self, MainWindow):
        MainWindow.setWindowTitle(QCoreApplication.translate("MainWindow", u"MainWindow", None))
        self.StartButton_1.setText(QCoreApplication.translate("MainWindow", u"\u0417\u0430\u043f\u0443\u0441\u043a", None))
        self.pushButton_2.setText(QCoreApplication.translate("MainWindow", u"test", None))
        self.ThreadLabel_1.setText(QCoreApplication.translate("MainWindow", u"\u041f\u043e\u0442\u043e\u043a\u0438", None))
        self.PauseCheckBox_1.setText(QCoreApplication.translate("MainWindow", u"\u041f\u0430\u0443\u0437\u0430", None))
        self.progressBar_1.setFormat(QCoreApplication.translate("MainWindow", u"%p%", None))
        self.ProgressLabel_1.setText(QCoreApplication.translate("MainWindow", u"0/0", None))
        self.TotalTimeLabel.setText(QCoreApplication.translate("MainWindow", u"[0:00:00]", None))
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.tab), QCoreApplication.translate("MainWindow", u"\u041e\u0431\u0440\u0430\u0431\u043e\u0442\u043a\u0430 \u043f\u0440\u0430\u0439\u0441\u043e\u0432", None))
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.tab_2), QCoreApplication.translate("MainWindow", u"\u041f\u043e\u0447\u0442\u0430", None))
    # retranslateUi

