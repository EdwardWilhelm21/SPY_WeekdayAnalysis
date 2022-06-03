#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 30 22:10:49 2022

@author: edwardwilhelm
"""

import pandas as pd
from datetime import date
import calendar
import yfinance as yf
from dfply import *

########################################
#User input start year to pull historic data from through current date of run
#User input to select Ticker to use

START_YEAR = "2022"
SECURITY_TICKER = 'SPY'

########################################
#Date formatting and setting YFinance ticker

CURR = str(date.today())
TICK = yf.Ticker(SECURITY_TICKER)
START_DATE = str(START_YEAR + "-01-01")
DATE_STRING = str(START_DATE + " --> " + CURR)

########################################
#Create pandas dataframe full of [Open, High, Low, Close, etc...]
hist = TICK.history(start=START_DATE, end=CURR)

#Calculate Day Change using DFPLY
day_change = hist >> transmute(Day_Change=X.Close - X.Open)

#Join in new dataframe
df = hist.join(day_change['Day_Change'])

#Take the future day's open and create column needed to calculate Night Change
#Done using DFPLY
open_lead = hist >> transmute(Open_Lead=lead(X.Open, 1))

#Join to dataframe
df = df.join(open_lead['Open_Lead'])

#Calculation for Night Change
night_change = df >> transmute(Night_Change=X.Open_Lead - X.Close)

#Join again
df = df.join(night_change['Night_Change'])

########################################
#Creating column to corrospond to that dates weekday
#Create list from dataframes index (That day's date)
dates = df.index.tolist()

#Create empty list to append weekday to
week_day = []

#Iterate trhough dates and append that day's corrosponding weekday
for date in dates:
    day = calendar.day_name[date.weekday()]
    week_day.append(day)
    
#Create new column in dataframe from corrosponding weekday
df['Week_Day'] = week_day

########################################
#Summary of Day Change by weekday
#Groups dataframe by weekday and takes average for each
#Done using DFPLY
day_change_summary = df >> group_by(X.Week_Day) >> summarize(Day_Change_Average=mean(X.Day_Change))

print('\n\tFROM: ' + START_DATE)

print('\n')
print('Day Summary: ')
print(day_change_summary)

#Plot on horizontal bar graph -> could be prettier
ax0 = day_change_summary.plot.barh(title= SECURITY_TICKER + ' Average Day Change\n by Weekday\n' + DATE_STRING, x='Week_Day', color='gray', grid=True, legend=False)

####################
#Summary of Night Change by weekday
#Groups dataframe by weekday and takes average for each
#Done using DFPLY
night_change_summary = df >> group_by(X.Week_Day) >> summarize(Night_Change_Average=mean(X.Night_Change))

print('\n')
print('Night Summary: ')
print(night_change_summary)

#Plot on horizontal bar graph -> could be prettier
ax1 = night_change_summary.plot.barh(title= SECURITY_TICKER + ' Average Night Change\n by Weekday\n' + DATE_STRING, x='Week_Day', color='gray', grid=True, legend=False)

########################################
#Analysis of Day/Night Change by count rather than average
#Create boolean dataframe columns if Day/Night change was green (positive)
df['Day_Green'] = df['Day_Change'] > 0
df['Night_Green'] = df['Night_Change'] > 0

#Group by weekday and take sum of boolean column. A True value will contribute a value of 1 to the sum
#Done using DFPLY
GreenDays = df >> group_by(X.Week_Day) >> mutate(Num_GreenDays=cumsum(X.Day_Green)) >> tail(1)
GreenNights = df >> group_by(X.Week_Day) >> mutate(Num_GreenNights=cumsum(X.Night_Green)) >> tail(1)

#Plot on bar graph -> could be prettier
ax2 = GreenDays.plot.bar(title= SECURITY_TICKER +' Number of Green Days\n by Weekday\n' + DATE_STRING, x='Week_Day', y='Num_GreenDays', color='green', grid=True, alpha=0.5, legend=False)
ax3 = GreenNights.plot.bar(title= SECURITY_TICKER +' Number of Green Nights\n by Weekday\n' + DATE_STRING, x='Week_Day', y='Num_GreenNights', color='green', grid=True, alpha=0.5, legend=False)

########################################
#What if you bought and sold one share of that stock every certain weekday for this timeframe?
#Buy 1 share at Open & sell that share at Close
#But only on Mondays, or Thursdays, etc.
DayTrades = df >> group_by(X.Week_Day) >> mutate(Temp_Return=cumsum(X.Day_Change)) >> tail(1) >> select(X.Week_Day, X.Temp_Return)

print('\n')
print('Day Trade Summary: ')
print(DayTrades)

#Plot on horizontal bar graph -> could be prettier
ax4 = DayTrades.plot.barh(title= SECURITY_TICKER +' Return on Day Trades\n by Weekday\n' + DATE_STRING, x='Week_Day', y='Temp_Return', color='gray', grid=True, legend=False)

####################
#Same thing but rather than a day trade you swing the share over night?
NightSwings = df >> group_by(X.Week_Day) >> mutate(Temp_Return=cumsum(X.Night_Change)) >> tail(1) >> select(X.Week_Day, X.Temp_Return)

print('\n')
print('Night Swing Summary: ')
print(NightSwings)

#Plot on horizontal bar graph -> could be prettier
ax5 = NightSwings.plot.barh(title= SECURITY_TICKER +' Return on Night Swings\n by Weekday\n' + DATE_STRING, x='Week_Day', y='Temp_Return', color='gray', grid=True, legend=False)













