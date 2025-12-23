@echo off
echo Testing Pipeline Connections...
echo.

echo 1. Testing Kafka...
curl -s http://localhost:9092 >nul 2>&1
if errorlevel 1 (
    echo ❌ Kafka not responding on localhost:9092
) else (
    echo ✅ Kafka is running
)

echo.
echo 2. Testing Tableau API...
curl -s http://localhost:4567/health
if errorlevel 1 (
    echo ❌ Tableau API not responding
) else (
    echo ✅ Tableau API is running
)

echo.
echo 3. Testing data endpoint...
curl -s http://localhost:4567/weather | findstr "temperature" >nul
if errorlevel 1 (
    echo ❌ No weather data available
) else (
    echo ✅ Weather data is flowing
)

echo.
echo 4. Checking running processes...
tasklist | findstr /i "java kafka" && echo ✅ Java/Kafka processes running

echo.
pause