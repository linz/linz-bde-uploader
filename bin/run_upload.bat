@echo off
rem Script to run the linz_bde_uploader perl script, rotating log files

setlocal enabledelayedexpansion
chdir /d %~dp0
set logdir=..\log
set scriptlog=linz_bde_uploader.log
set runlog=linz_bde_uploader_run.log
set last=11

for %%f in (10 9 8 7 6 5 4 3 2 1) do (
   del /f /q %logdir%\%scriptlog%.!last! >nul 2>&1
   del /f /q %logdir%\%runlog%.!last! >nul 2>&1
   rename %logdir%\%scriptlog%.%%f %scriptlog%.!last! >nul 2>&1
   rename %logdir%\%runlog%.%%f %runlog%.!last! >nul 2>&1
   set last=%%f

)
rename %logdir%\%scriptlog% %scriptlog%.1 >nul 2>&1
rename %logdir%\%runlog% %runlog%.1 >nul 2>&1

perl linz_bde_uploader.pl -verbose -listing %logdir%\%scriptlog% %* >%logdir%\%runlog% 2>&1
