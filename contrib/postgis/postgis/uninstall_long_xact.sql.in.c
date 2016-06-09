-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- 
-- $Id: uninstall_long_xact.sql.in.c 9324 2012-02-27 22:08:12Z pramsey $
--
-- PostGIS - Spatial Types for PostgreSQL
-- http://postgis.refractions.net
-- Copyright 2001-2003 Refractions Research Inc.
--
-- This is free software; you can redistribute and/or modify it under
-- the terms of the GNU General Public Licence. See the COPYING file.
--  
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -


-----------------------------------------------------------------------
-- LONG TERM LOCKING
-----------------------------------------------------------------------

DROP FUNCTION UnlockRows(text);
DROP FUNCTION LockRow(text, text, text, text, timestamp);
DROP FUNCTION LockRow(text, text, text, text);
DROP FUNCTION LockRow(text, text, text);
DROP FUNCTION LockRow(text, text, text, timestamp);
DROP FUNCTION AddAuth(text);
DROP FUNCTION CheckAuth(text, text, text);
DROP FUNCTION CheckAuth(text, text);
DROP FUNCTION CheckAuthTrigger();
DROP FUNCTION GetTransactionID();
DROP FUNCTION EnableLongTransactions();
DROP FUNCTION LongTransactionsEnabled();
DROP FUNCTION DisableLongTransactions();

