REM $Id: upgrade_geocoder.bat 9324 2012-02-27 22:08:12Z pramsey $
set PGPORT=5432
set PGHOST=localhost
set PGUSER=postgres
set PGPASSWORD=yourpasswordhere
set THEDB=geocoder
set PGBIN=C:\Program Files\PostgreSQL\8.4\bin
set PGCONTRIB=C:\Program Files\PostgreSQL\8.4\share\contrib
REM "%PGBIN%\psql"  -d "%THEDB%" -f "tiger_loader.sql"
"%PGBIN%\psql"  -d "%THEDB%" -f "upgrade_geocode.sql"
cd regress
"%PGBIN%\psql" -t -f regress.sql
pause

