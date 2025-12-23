@echo off
echo Stopping Kafka Weather Pipeline...
taskkill /FI "WINDOWTITLE eq Kafka Server" /F
taskkill /FI "WINDOWTITLE eq Weather Producer" /F
taskkill /FI "WINDOWTITLE eq ETL Consumer" /F
taskkill /FI "WINDOWTITLE eq Tableau API" /F
echo All services stopped.
pause