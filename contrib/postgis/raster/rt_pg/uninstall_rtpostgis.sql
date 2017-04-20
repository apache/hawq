-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
-- 
-- PostGIS - Spatial Types for PostgreSQL 
-- http://postgis.refractions.net 
-- 
-- This is free software; you can redistribute and/or modify it under 
-- the terms of the GNU General Public Licence. See the COPYING file. 
-- 
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
-- 
-- Generated on: Wed Jun  8 20:08:48 2016
--           by: ../../utils/create_undef.pl
--         from: rtpostgis.sql
-- 
-- Do not edit manually, your changes will be lost.
-- 
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

BEGIN;

-- Drop all views.
DROP VIEW IF EXISTS raster_columns;
DROP VIEW IF EXISTS raster_overviews;
-- Drop all tables.
-- Drop all aggregates.
DROP AGGREGATE IF EXISTS ST_Union (raster);
DROP AGGREGATE IF EXISTS ST_Union (raster, integer);
DROP AGGREGATE IF EXISTS ST_Union (raster, text);
DROP AGGREGATE IF EXISTS ST_Union (raster, integer, text);
-- Drop all operators classes and families.
-- Drop all operators.
DROP OPERATOR <<  (raster,raster) CASCADE;
DROP OPERATOR &<  (raster,raster) CASCADE;
DROP OPERATOR <<|  (raster,raster) CASCADE;
DROP OPERATOR &<|  (raster,raster) CASCADE;
DROP OPERATOR &&  (raster,raster) CASCADE;
DROP OPERATOR &>  (raster,raster) CASCADE;
DROP OPERATOR >>  (raster,raster) CASCADE;
DROP OPERATOR |&>  (raster,raster) CASCADE;
DROP OPERATOR |>>  (raster,raster) CASCADE;
DROP OPERATOR ~=  (raster,raster) CASCADE;
DROP OPERATOR @  (raster,raster) CASCADE;
DROP OPERATOR ~  (raster,raster) CASCADE;
DROP OPERATOR ~  (raster,geometry) CASCADE;
DROP OPERATOR &&  (raster,geometry) CASCADE;
DROP OPERATOR ~  (geometry,raster) CASCADE;
DROP OPERATOR &&  (geometry,raster) CASCADE;
-- Drop all casts.
DROP CAST (raster AS box3d);
DROP CAST (raster AS geometry);
DROP CAST (raster AS bytea);
-- Drop all functions except 2 needed for type definition.
DROP FUNCTION IF EXISTS postgis_raster_lib_version ();
DROP FUNCTION IF EXISTS postgis_raster_scripts_installed ();
DROP FUNCTION IF EXISTS postgis_raster_lib_build_date ();
DROP FUNCTION IF EXISTS postgis_gdal_version ();
DROP FUNCTION IF EXISTS st_convexhull (raster);
DROP FUNCTION IF EXISTS box3d (raster);
DROP FUNCTION IF EXISTS st_envelope (raster);
DROP FUNCTION IF EXISTS st_height (raster);
DROP FUNCTION IF EXISTS st_numbands (raster);
DROP FUNCTION IF EXISTS st_scalex (raster);
DROP FUNCTION IF EXISTS st_scaley (raster);
DROP FUNCTION IF EXISTS st_skewx (raster);
DROP FUNCTION IF EXISTS st_skewy (raster);
DROP FUNCTION IF EXISTS st_srid (raster);
DROP FUNCTION IF EXISTS st_upperleftx (raster);
DROP FUNCTION IF EXISTS st_upperlefty (raster);
DROP FUNCTION IF EXISTS st_width (raster);
DROP FUNCTION IF EXISTS st_pixelwidth (raster);
DROP FUNCTION IF EXISTS st_pixelheight (raster);
DROP FUNCTION IF EXISTS st_geotransform (raster,
    OUT imag double precision,
    OUT jmag double precision,
    OUT theta_i double precision,
    OUT theta_ij double precision,
    OUT xoffset double precision,
    OUT yoffset double precision);
DROP FUNCTION IF EXISTS st_rotation (raster);
DROP FUNCTION IF EXISTS st_metadata (
	rast raster,
	OUT upperleftx double precision,
	OUT upperlefty double precision,
	OUT width int,
	OUT height int,
	OUT scalex double precision,
	OUT scaley double precision,
	OUT skewx double precision,
	OUT skewy double precision,
	OUT srid int,
	OUT numbands int
);
DROP FUNCTION IF EXISTS st_makeemptyraster (width int, height int, upperleftx float8, upperlefty float8, scalex float8, scaley float8, skewx float8, skewy float8, srid int4 );
DROP FUNCTION IF EXISTS st_makeemptyraster (width int, height int, upperleftx float8, upperlefty float8, pixelsize float8);
DROP FUNCTION IF EXISTS st_makeemptyraster (rast raster);
DROP FUNCTION IF EXISTS st_addband (rast raster, index int, pixeltype text, initialvalue float8 , nodataval float8 );
DROP FUNCTION IF EXISTS st_addband (rast raster, pixeltype text, initialvalue float8 , nodataval float8 );
DROP FUNCTION IF EXISTS st_addband (torast raster, fromrast raster, fromband int , torastindex int );
DROP FUNCTION IF EXISTS ST_AddBand (torast raster, fromrasts raster[], fromband integer );
DROP FUNCTION IF EXISTS st_band (rast raster, nbands int[] );
DROP FUNCTION IF EXISTS st_band (rast raster, nband int);
DROP FUNCTION IF EXISTS st_band (rast raster, nbands text, delimiter char );
DROP FUNCTION IF EXISTS _st_summarystats (rast raster, nband int , exclude_nodata_value boolean , sample_percent double precision );
DROP FUNCTION IF EXISTS st_summarystats (rast raster, nband int , exclude_nodata_value boolean );
DROP FUNCTION IF EXISTS st_summarystats (rast raster, exclude_nodata_value boolean);
DROP FUNCTION IF EXISTS st_approxsummarystats (rast raster, nband int , exclude_nodata_value boolean , sample_percent double precision );
DROP FUNCTION IF EXISTS st_approxsummarystats (rast raster, nband int, sample_percent double precision);
DROP FUNCTION IF EXISTS st_approxsummarystats (rast raster, exclude_nodata_value boolean, sample_percent double precision );
DROP FUNCTION IF EXISTS st_approxsummarystats (rast raster, sample_percent double precision);
DROP FUNCTION IF EXISTS _st_summarystats (rastertable text, rastercolumn text, nband integer , exclude_nodata_value boolean , sample_percent double precision );
DROP FUNCTION IF EXISTS st_summarystats (rastertable text, rastercolumn text, nband integer , exclude_nodata_value boolean );
DROP FUNCTION IF EXISTS st_summarystats (rastertable text, rastercolumn text, exclude_nodata_value boolean);
DROP FUNCTION IF EXISTS st_approxsummarystats (rastertable text, rastercolumn text, nband integer , exclude_nodata_value boolean , sample_percent double precision );
DROP FUNCTION IF EXISTS st_approxsummarystats (rastertable text, rastercolumn text, nband integer, sample_percent double precision);
DROP FUNCTION IF EXISTS st_approxsummarystats (rastertable text, rastercolumn text, exclude_nodata_value boolean);
DROP FUNCTION IF EXISTS st_approxsummarystats (rastertable text, rastercolumn text, sample_percent double precision);
DROP FUNCTION IF EXISTS _st_count (rast raster, nband int , exclude_nodata_value boolean , sample_percent double precision );
DROP FUNCTION IF EXISTS st_count (rast raster, nband int , exclude_nodata_value boolean );
DROP FUNCTION IF EXISTS st_count (rast raster, exclude_nodata_value boolean);
DROP FUNCTION IF EXISTS st_approxcount (rast raster, nband int , exclude_nodata_value boolean , sample_percent double precision );
DROP FUNCTION IF EXISTS st_approxcount (rast raster, nband int, sample_percent double precision);
DROP FUNCTION IF EXISTS st_approxcount (rast raster, exclude_nodata_value boolean, sample_percent double precision );
DROP FUNCTION IF EXISTS st_approxcount (rast raster, sample_percent double precision);
DROP FUNCTION IF EXISTS _st_count (rastertable text, rastercolumn text, nband integer , exclude_nodata_value boolean , sample_percent double precision );
DROP FUNCTION IF EXISTS st_count (rastertable text, rastercolumn text, nband int , exclude_nodata_value boolean );
DROP FUNCTION IF EXISTS st_count (rastertable text, rastercolumn text, exclude_nodata_value boolean);
DROP FUNCTION IF EXISTS st_approxcount (rastertable text, rastercolumn text, nband int , exclude_nodata_value boolean , sample_percent double precision );
DROP FUNCTION IF EXISTS st_approxcount (rastertable text, rastercolumn text, nband int, sample_percent double precision);
DROP FUNCTION IF EXISTS st_approxcount (rastertable text, rastercolumn text, exclude_nodata_value boolean, sample_percent double precision );
DROP FUNCTION IF EXISTS st_approxcount (rastertable text, rastercolumn text, sample_percent double precision);
DROP FUNCTION IF EXISTS _st_histogram (
	rast raster, nband int ,
	exclude_nodata_value boolean ,
	sample_percent double precision ,
	bins int , width double precision[] ,
	right boolean ,
	min double precision , max double precision );
DROP FUNCTION IF EXISTS st_histogram (rast raster, nband int , exclude_nodata_value boolean , bins int , width double precision[] , right boolean );
DROP FUNCTION IF EXISTS st_histogram (rast raster, nband int, exclude_nodata_value boolean, bins int, right boolean);
DROP FUNCTION IF EXISTS st_histogram (rast raster, nband int, bins int, width double precision[] , right boolean );
DROP FUNCTION IF EXISTS st_histogram (rast raster, nband int, bins int, right boolean);
DROP FUNCTION IF EXISTS st_approxhistogram (
	rast raster, nband int ,
	exclude_nodata_value boolean ,
	sample_percent double precision ,
	bins int , width double precision[] ,
	right boolean );
DROP FUNCTION IF EXISTS st_approxhistogram (rast raster, nband int, exclude_nodata_value boolean, sample_percent double precision, bins int, right boolean);
DROP FUNCTION IF EXISTS st_approxhistogram (rast raster, nband int, sample_percent double precision);
DROP FUNCTION IF EXISTS st_approxhistogram (rast raster, sample_percent double precision);
DROP FUNCTION IF EXISTS st_approxhistogram (rast raster, nband int, sample_percent double precision, bins int, width double precision[] , right boolean );
DROP FUNCTION IF EXISTS st_approxhistogram (rast raster, nband int, sample_percent double precision, bins int, right boolean);
DROP FUNCTION IF EXISTS _st_histogram (
	rastertable text, rastercolumn text,
	nband int ,
	exclude_nodata_value boolean ,
	sample_percent double precision ,
	bins int , width double precision[] ,
	right boolean );
DROP FUNCTION IF EXISTS st_histogram (rastertable text, rastercolumn text, nband int , exclude_nodata_value boolean , bins int , width double precision[] , right boolean );
DROP FUNCTION IF EXISTS st_histogram (rastertable text, rastercolumn text, nband int, exclude_nodata_value boolean, bins int, right boolean);
DROP FUNCTION IF EXISTS st_histogram (rastertable text, rastercolumn text, nband int, bins int, width double precision[] , right boolean );
DROP FUNCTION IF EXISTS st_histogram (rastertable text, rastercolumn text, nband int, bins int, right boolean);
DROP FUNCTION IF EXISTS st_approxhistogram (
	rastertable text, rastercolumn text,
	nband int ,
	exclude_nodata_value boolean ,
	sample_percent double precision ,
	bins int , width double precision[] ,
	right boolean );
DROP FUNCTION IF EXISTS st_approxhistogram (rastertable text, rastercolumn text, nband int, exclude_nodata_value boolean, sample_percent double precision, bins int, right boolean);
DROP FUNCTION IF EXISTS st_approxhistogram (rastertable text, rastercolumn text, nband int, sample_percent double precision);
DROP FUNCTION IF EXISTS st_approxhistogram (rastertable text, rastercolumn text, sample_percent double precision);
DROP FUNCTION IF EXISTS st_approxhistogram (rastertable text, rastercolumn text, nband int, sample_percent double precision, bins int, width double precision[] , right boolean );
DROP FUNCTION IF EXISTS st_approxhistogram (rastertable text, rastercolumn text, nband int, sample_percent double precision, bins int, right boolean);
DROP FUNCTION IF EXISTS _st_quantile (rast raster, nband int , exclude_nodata_value boolean , sample_percent double precision , quantiles double precision[] );
DROP FUNCTION IF EXISTS st_quantile (rast raster, nband int , exclude_nodata_value boolean , quantiles double precision[] );
DROP FUNCTION IF EXISTS st_quantile (rast raster, nband int, quantiles double precision[]);
DROP FUNCTION IF EXISTS st_quantile (rast raster, quantiles double precision[]);
DROP FUNCTION IF EXISTS st_quantile (rast raster, nband int, exclude_nodata_value boolean, quantile double precision);
DROP FUNCTION IF EXISTS st_quantile (rast raster, nband int, quantile double precision);
DROP FUNCTION IF EXISTS st_quantile (rast raster, exclude_nodata_value boolean, quantile double precision );
DROP FUNCTION IF EXISTS st_quantile (rast raster, quantile double precision);
DROP FUNCTION IF EXISTS st_approxquantile (rast raster, nband int , exclude_nodata_value boolean , sample_percent double precision , quantiles double precision[] );
DROP FUNCTION IF EXISTS st_approxquantile (rast raster, nband int, sample_percent double precision, quantiles double precision[] );
DROP FUNCTION IF EXISTS st_approxquantile (rast raster, sample_percent double precision, quantiles double precision[] );
DROP FUNCTION IF EXISTS st_approxquantile (rast raster, quantiles double precision[]);
DROP FUNCTION IF EXISTS st_approxquantile (rast raster, nband int, exclude_nodata_value boolean, sample_percent double precision, quantile double precision);
DROP FUNCTION IF EXISTS st_approxquantile (rast raster, nband int, sample_percent double precision, quantile double precision);
DROP FUNCTION IF EXISTS st_approxquantile (rast raster, sample_percent double precision, quantile double precision);
DROP FUNCTION IF EXISTS st_approxquantile (rast raster, exclude_nodata_value boolean, quantile double precision );
DROP FUNCTION IF EXISTS st_approxquantile (rast raster, quantile double precision);
DROP FUNCTION IF EXISTS _st_quantile (rastertable text, rastercolumn text, nband int , exclude_nodata_value boolean , sample_percent double precision , quantiles double precision[] );
DROP FUNCTION IF EXISTS st_quantile (rastertable text, rastercolumn text, nband int , exclude_nodata_value boolean , quantiles double precision[] );
DROP FUNCTION IF EXISTS st_quantile (rastertable text, rastercolumn text, nband int, quantiles double precision[]);
DROP FUNCTION IF EXISTS st_quantile (rastertable text, rastercolumn text, quantiles double precision[]);
DROP FUNCTION IF EXISTS st_quantile (rastertable text, rastercolumn text, nband int, exclude_nodata_value boolean, quantile double precision);
DROP FUNCTION IF EXISTS st_quantile (rastertable text, rastercolumn text, nband int, quantile double precision);
DROP FUNCTION IF EXISTS st_quantile (rastertable text, rastercolumn text, exclude_nodata_value boolean, quantile double precision );
DROP FUNCTION IF EXISTS st_quantile (rastertable text, rastercolumn text, quantile double precision);
DROP FUNCTION IF EXISTS st_approxquantile (rastertable text, rastercolumn text, nband int , exclude_nodata_value boolean , sample_percent double precision , quantiles double precision[] );
DROP FUNCTION IF EXISTS st_approxquantile (rastertable text, rastercolumn text, nband int, sample_percent double precision, quantiles double precision[] );
DROP FUNCTION IF EXISTS st_approxquantile (rastertable text, rastercolumn text, sample_percent double precision, quantiles double precision[] );
DROP FUNCTION IF EXISTS st_approxquantile (rastertable text, rastercolumn text, quantiles double precision[]);
DROP FUNCTION IF EXISTS st_approxquantile (rastertable text, rastercolumn text, nband int, exclude_nodata_value boolean, sample_percent double precision, quantile double precision);
DROP FUNCTION IF EXISTS st_approxquantile (rastertable text, rastercolumn text, nband int, sample_percent double precision, quantile double precision);
DROP FUNCTION IF EXISTS st_approxquantile (rastertable text, rastercolumn text, sample_percent double precision, quantile double precision);
DROP FUNCTION IF EXISTS st_approxquantile (rastertable text, rastercolumn text, exclude_nodata_value boolean, quantile double precision );
DROP FUNCTION IF EXISTS st_approxquantile (rastertable text, rastercolumn text, quantile double precision);
DROP FUNCTION IF EXISTS _st_valuecount (rast raster, nband integer , exclude_nodata_value boolean , searchvalues double precision[] , roundto double precision );
DROP FUNCTION IF EXISTS st_valuecount (
	rast raster, nband integer ,
	exclude_nodata_value boolean ,
	searchvalues double precision[] ,
	roundto double precision ,
	OUT value double precision, OUT count integer
);
DROP FUNCTION IF EXISTS st_valuecount (rast raster, nband integer, searchvalues double precision[], roundto double precision , OUT value double precision, OUT count integer);
DROP FUNCTION IF EXISTS st_valuecount (rast raster, searchvalues double precision[], roundto double precision , OUT value double precision, OUT count integer);
DROP FUNCTION IF EXISTS st_valuecount (rast raster, nband integer, exclude_nodata_value boolean, searchvalue double precision, roundto double precision );
DROP FUNCTION IF EXISTS st_valuecount (rast raster, nband integer, searchvalue double precision, roundto double precision );
DROP FUNCTION IF EXISTS st_valuecount (rast raster, searchvalue double precision, roundto double precision );
DROP FUNCTION IF EXISTS st_valuepercent (
	rast raster, nband integer ,
	exclude_nodata_value boolean ,
	searchvalues double precision[] ,
	roundto double precision ,
	OUT value double precision, OUT percent double precision
);
DROP FUNCTION IF EXISTS st_valuepercent (rast raster, nband integer, searchvalues double precision[], roundto double precision , OUT value double precision, OUT percent double precision);
DROP FUNCTION IF EXISTS st_valuepercent (rast raster, searchvalues double precision[], roundto double precision , OUT value double precision, OUT percent double precision);
DROP FUNCTION IF EXISTS st_valuepercent (rast raster, nband integer, exclude_nodata_value boolean, searchvalue double precision, roundto double precision );
DROP FUNCTION IF EXISTS st_valuepercent (rast raster, nband integer, searchvalue double precision, roundto double precision );
DROP FUNCTION IF EXISTS st_valuepercent (rast raster, searchvalue double precision, roundto double precision );
DROP FUNCTION IF EXISTS _st_valuecount (rastertable text, rastercolumn text, nband integer , exclude_nodata_value boolean , searchvalues double precision[] , roundto double precision );
DROP FUNCTION IF EXISTS st_valuecount (
	rastertable text, rastercolumn text,
	nband integer ,
	exclude_nodata_value boolean ,
	searchvalues double precision[] ,
	roundto double precision ,
	OUT value double precision, OUT count integer
);
DROP FUNCTION IF EXISTS st_valuecount (rastertable text, rastercolumn text, nband integer, searchvalues double precision[], roundto double precision , OUT value double precision, OUT count integer);
DROP FUNCTION IF EXISTS st_valuecount (rastertable text, rastercolumn text, searchvalues double precision[], roundto double precision , OUT value double precision, OUT count integer);
DROP FUNCTION IF EXISTS st_valuecount (rastertable text, rastercolumn text, nband integer, exclude_nodata_value boolean, searchvalue double precision, roundto double precision );
DROP FUNCTION IF EXISTS st_valuecount (rastertable text, rastercolumn text, nband integer, searchvalue double precision, roundto double precision );
DROP FUNCTION IF EXISTS st_valuecount (rastertable text, rastercolumn text, searchvalue double precision, roundto double precision );
DROP FUNCTION IF EXISTS st_valuepercent (
	rastertable text, rastercolumn text,
	nband integer ,
	exclude_nodata_value boolean ,
	searchvalues double precision[] ,
	roundto double precision ,
	OUT value double precision, OUT percent double precision
);
DROP FUNCTION IF EXISTS st_valuepercent (rastertable text, rastercolumn text, nband integer, searchvalues double precision[], roundto double precision , OUT value double precision, OUT percent double precision);
DROP FUNCTION IF EXISTS st_valuepercent (rastertable text, rastercolumn text, searchvalues double precision[], roundto double precision , OUT value double precision, OUT percent double precision);
DROP FUNCTION IF EXISTS st_valuepercent (rastertable text, rastercolumn text, nband integer, exclude_nodata_value boolean, searchvalue double precision, roundto double precision );
DROP FUNCTION IF EXISTS st_valuepercent (rastertable text, rastercolumn text, nband integer, searchvalue double precision, roundto double precision );
DROP FUNCTION IF EXISTS st_valuepercent (rastertable text, rastercolumn text, searchvalue double precision, roundto double precision );
DROP FUNCTION IF EXISTS _st_reclass (rast raster, VARIADIC reclassargset reclassarg[]);
DROP FUNCTION IF EXISTS st_reclass (rast raster, VARIADIC reclassargset reclassarg[]);
DROP FUNCTION IF EXISTS st_reclass (rast raster, nband int, reclassexpr text, pixeltype text, nodataval double precision );
DROP FUNCTION IF EXISTS st_reclass (rast raster, reclassexpr text, pixeltype text);
DROP FUNCTION IF EXISTS st_gdaldrivers (OUT idx int, OUT short_name text, OUT long_name text, OUT create_options text);
DROP FUNCTION IF EXISTS st_asgdalraster (rast raster, format text, options text[] , srid integer );
DROP FUNCTION IF EXISTS st_astiff (rast raster, options text[] , srid integer );
DROP FUNCTION IF EXISTS st_astiff (rast raster, nbands int[], options text[] , srid integer );
DROP FUNCTION IF EXISTS st_astiff (rast raster, compression text, srid integer );
DROP FUNCTION IF EXISTS st_astiff (rast raster, nbands int[], compression text, srid integer );
DROP FUNCTION IF EXISTS st_asjpeg (rast raster, options text[] );
DROP FUNCTION IF EXISTS st_asjpeg (rast raster, nbands int[], options text[] );
DROP FUNCTION IF EXISTS st_asjpeg (rast raster, nbands int[], quality int);
DROP FUNCTION IF EXISTS st_asjpeg (rast raster, nband int, options text[] );
DROP FUNCTION IF EXISTS st_asjpeg (rast raster, nband int, quality int);
DROP FUNCTION IF EXISTS st_aspng (rast raster, options text[] );
DROP FUNCTION IF EXISTS st_aspng (rast raster, nbands int[], options text[] );
DROP FUNCTION IF EXISTS st_aspng (rast raster, nbands int[], compression int);
DROP FUNCTION IF EXISTS st_aspng (rast raster, nband int, options text[] );
DROP FUNCTION IF EXISTS st_aspng (rast raster, nband int, compression int);
DROP FUNCTION IF EXISTS _st_asraster (
	geom geometry,
	scalex double precision , scaley double precision ,
	width integer , height integer ,
	pixeltype text[] ,
	value double precision[] ,
	nodataval double precision[] ,
	upperleftx double precision , upperlefty double precision ,
	gridx double precision , gridy double precision ,
	skewx double precision , skewy double precision ,
	touched boolean );
DROP FUNCTION IF EXISTS st_asraster (
	geom geometry,
	scalex double precision, scaley double precision,
	gridx double precision , gridy double precision ,
	pixeltype text[] ,
	value double precision[] ,
	nodataval double precision[] ,
	skewx double precision , skewy double precision ,
	touched boolean );
DROP FUNCTION IF EXISTS st_asraster (
	geom geometry,
	scalex double precision, scaley double precision,
	pixeltype text[],
	value double precision[] ,
	nodataval double precision[] ,
	upperleftx double precision , upperlefty double precision ,
	skewx double precision , skewy double precision ,
	touched boolean );
DROP FUNCTION IF EXISTS st_asraster (
	geom geometry,
	width integer, height integer,
	gridx double precision , gridy double precision ,
	pixeltype text[] ,
	value double precision[] ,
	nodataval double precision[] ,
	skewx double precision , skewy double precision ,
	touched boolean );
DROP FUNCTION IF EXISTS st_asraster (
	geom geometry,
	width integer, height integer,
	pixeltype text[],
	value double precision[] ,
	nodataval double precision[] ,
	upperleftx double precision , upperlefty double precision ,
	skewx double precision , skewy double precision ,
	touched boolean );
DROP FUNCTION IF EXISTS st_asraster (
	geom geometry,
	scalex double precision, scaley double precision,
	gridx double precision, gridy double precision,
	pixeltype text,
	value double precision ,
	nodataval double precision ,
	skewx double precision , skewy double precision ,
	touched boolean );
DROP FUNCTION IF EXISTS st_asraster (
	geom geometry,
	scalex double precision, scaley double precision,
	pixeltype text,
	value double precision ,
	nodataval double precision ,
	upperleftx double precision , upperlefty double precision ,
	skewx double precision , skewy double precision ,
	touched boolean );
DROP FUNCTION IF EXISTS st_asraster (
	geom geometry,
	width integer, height integer,
	gridx double precision, gridy double precision,
	pixeltype text,
	value double precision ,
	nodataval double precision ,
	skewx double precision , skewy double precision ,
	touched boolean );
DROP FUNCTION IF EXISTS st_asraster (
	geom geometry,
	width integer, height integer,
	pixeltype text,
	value double precision ,
	nodataval double precision ,
	upperleftx double precision , upperlefty double precision ,
	skewx double precision , skewy double precision ,
	touched boolean );
DROP FUNCTION IF EXISTS st_asraster (
	geom geometry,
	ref raster,
	pixeltype text[] ,
	value double precision[] ,
	nodataval double precision[] ,
	touched boolean );
DROP FUNCTION IF EXISTS st_asraster (
	geom geometry,
	ref raster,
	pixeltype text,
	value double precision ,
	nodataval double precision ,
	touched boolean );
DROP FUNCTION IF EXISTS _st_resample (
	rast raster,
	algorithm text , maxerr double precision ,
	srid integer ,
	scalex double precision , scaley double precision ,
	gridx double precision , gridy double precision ,
	skewx double precision , skewy double precision ,
	width integer , height integer );
DROP FUNCTION IF EXISTS st_resample (
	rast raster,
	srid integer ,
	scalex double precision , scaley double precision ,
	gridx double precision , gridy double precision ,
	skewx double precision , skewy double precision ,
	algorithm text , maxerr double precision );
DROP FUNCTION IF EXISTS st_resample (
	rast raster,
	width integer, height integer,
	srid integer ,
	gridx double precision , gridy double precision ,
	skewx double precision , skewy double precision ,
	algorithm text , maxerr double precision );
DROP FUNCTION IF EXISTS st_resample (
	rast raster,
	ref raster,
	algorithm text ,
	maxerr double precision ,
	usescale boolean );
DROP FUNCTION IF EXISTS st_resample (
	rast raster,
	ref raster,
	usescale boolean,
	algorithm text ,
	maxerr double precision );
DROP FUNCTION IF EXISTS st_transform (rast raster, srid integer, algorithm text , maxerr double precision , scalex double precision , scaley double precision );
DROP FUNCTION IF EXISTS st_transform (rast raster, srid integer, scalex double precision, scaley double precision, algorithm text , maxerr double precision );
DROP FUNCTION IF EXISTS st_transform (rast raster, srid integer, scalexy double precision, algorithm text , maxerr double precision );
DROP FUNCTION IF EXISTS st_rescale (rast raster, scalex double precision, scaley double precision, algorithm text , maxerr double precision );
DROP FUNCTION IF EXISTS st_rescale (rast raster, scalexy double precision, algorithm text , maxerr double precision );
DROP FUNCTION IF EXISTS st_reskew (rast raster, skewx double precision, skewy double precision, algorithm text , maxerr double precision );
DROP FUNCTION IF EXISTS st_reskew (rast raster, skewxy double precision, algorithm text , maxerr double precision );
DROP FUNCTION IF EXISTS st_snaptogrid (
	rast raster,
	gridx double precision, gridy double precision,
	algorithm text , maxerr double precision ,
	scalex double precision , scaley double precision );
DROP FUNCTION IF EXISTS st_snaptogrid (
	rast raster,
	gridx double precision, gridy double precision,
	scalex double precision, scaley double precision,
	algorithm text , maxerr double precision );
DROP FUNCTION IF EXISTS st_snaptogrid (
	rast raster,
	gridx double precision, gridy double precision,
	scalexy double precision,
	algorithm text , maxerr double precision );
DROP FUNCTION IF EXISTS st_mapalgebraexpr (rast raster, band integer, pixeltype text,
        expression text, nodataval double precision );
DROP FUNCTION IF EXISTS st_mapalgebraexpr (rast raster, pixeltype text, expression text,
        nodataval double precision );
DROP FUNCTION IF EXISTS st_mapalgebrafct (rast raster, band integer,
        pixeltype text, onerastuserfunc regprocedure, variadic args text[]);
DROP FUNCTION IF EXISTS st_mapalgebrafct (rast raster, band integer,
        pixeltype text, onerastuserfunc regprocedure);
DROP FUNCTION IF EXISTS st_mapalgebrafct (rast raster, band integer,
        onerastuserfunc regprocedure, variadic args text[]);
DROP FUNCTION IF EXISTS st_mapalgebrafct (rast raster, band integer,
        onerastuserfunc regprocedure);
DROP FUNCTION IF EXISTS st_mapalgebrafct (rast raster, pixeltype text,
        onerastuserfunc regprocedure, variadic args text[]);
DROP FUNCTION IF EXISTS st_mapalgebrafct (rast raster, pixeltype text,
        onerastuserfunc regprocedure);
DROP FUNCTION IF EXISTS st_mapalgebrafct (rast raster, onerastuserfunc regprocedure,
        variadic args text[]);
DROP FUNCTION IF EXISTS st_mapalgebrafct (rast raster, onerastuserfunc regprocedure);
DROP FUNCTION IF EXISTS st_mapalgebraexpr (
	rast1 raster, band1 integer,
	rast2 raster, band2 integer,
	expression text,
	pixeltype text , extenttype text ,
	nodata1expr text , nodata2expr text ,
	nodatanodataval double precision );
DROP FUNCTION IF EXISTS st_mapalgebraexpr (
	rast1 raster,
	rast2 raster,
	expression text,
	pixeltype text , extenttype text ,
	nodata1expr text , nodata2expr text ,
	nodatanodataval double precision );
DROP FUNCTION IF EXISTS st_mapalgebrafct (
	rast1 raster, band1 integer,
	rast2 raster, band2 integer,
	tworastuserfunc regprocedure,
	pixeltype text , extenttype text ,
	VARIADIC userargs text[] );
DROP FUNCTION IF EXISTS st_mapalgebrafct (
	rast1 raster,
	rast2 raster,
	tworastuserfunc regprocedure,
	pixeltype text , extenttype text ,
	VARIADIC userargs text[] );
DROP FUNCTION IF EXISTS st_mapalgebrafctngb (
    rast raster,
    band integer,
    pixeltype text,
    ngbwidth integer,
    ngbheight integer,
    onerastngbuserfunc regprocedure,
    nodatamode text,
    variadic args text[]
);
DROP FUNCTION IF EXISTS st_max4ma (matrix float[][], nodatamode text, variadic args text[]);
DROP FUNCTION IF EXISTS st_min4ma (matrix float[][], nodatamode text, variadic args text[]);
DROP FUNCTION IF EXISTS st_sum4ma (matrix float[][], nodatamode text, variadic args text[]);
DROP FUNCTION IF EXISTS st_mean4ma (matrix float[][], nodatamode text, variadic args text[]);
DROP FUNCTION IF EXISTS st_range4ma (matrix float[][], nodatamode text, variadic args text[]);
DROP FUNCTION IF EXISTS _st_slope4ma (matrix float[][], nodatamode text, variadic args text[]);
DROP FUNCTION IF EXISTS st_slope (rast raster, band integer, pixeltype text);
DROP FUNCTION IF EXISTS _st_aspect4ma (matrix float[][], nodatamode text, variadic args text[]);
DROP FUNCTION IF EXISTS st_aspect (rast raster, band integer, pixeltype text);
DROP FUNCTION IF EXISTS _st_hillshade4ma (matrix float[][], nodatamode text, variadic args text[]);
DROP FUNCTION IF EXISTS st_hillshade (rast raster, band integer, pixeltype text, azimuth float, altitude float, max_bright float , elevation_scale float );
DROP FUNCTION IF EXISTS st_distinct4ma (matrix float[][], nodatamode TEXT, VARIADIC args TEXT[]);
DROP FUNCTION IF EXISTS st_stddev4ma (matrix float[][], nodatamode TEXT, VARIADIC args TEXT[]);
DROP FUNCTION IF EXISTS st_isempty (rast raster);
DROP FUNCTION IF EXISTS st_hasnoband (rast raster, nband int );
DROP FUNCTION IF EXISTS st_bandnodatavalue (rast raster, band integer );
DROP FUNCTION IF EXISTS st_bandisnodata (rast raster, band integer , forceChecking boolean );
DROP FUNCTION IF EXISTS st_bandisnodata (rast raster, forceChecking boolean);
DROP FUNCTION IF EXISTS st_bandpath (rast raster, band integer );
DROP FUNCTION IF EXISTS st_bandpixeltype (rast raster, band integer );
DROP FUNCTION IF EXISTS st_bandmetadata (
	rast raster,
	band int[],
	OUT bandnum int,
	OUT pixeltype text,
	OUT nodatavalue double precision,
	OUT isoutdb boolean,
	OUT path text
);
DROP FUNCTION IF EXISTS st_bandmetadata (
	rast raster,
	band int ,
	OUT pixeltype text,
	OUT nodatavalue double precision,
	OUT isoutdb boolean,
	OUT path text
);
DROP FUNCTION IF EXISTS st_value (rast raster, band integer, x integer, y integer, hasnodata boolean );
DROP FUNCTION IF EXISTS st_value (rast raster, x integer, y integer, hasnodata boolean );
DROP FUNCTION IF EXISTS st_value (rast raster, band integer, pt geometry, hasnodata boolean );
DROP FUNCTION IF EXISTS st_value (rast raster, pt geometry, hasnodata boolean );
DROP FUNCTION IF EXISTS st_georeference (rast raster, format text );
DROP FUNCTION IF EXISTS st_setscale (rast raster, scale float8);
DROP FUNCTION IF EXISTS st_setscale (rast raster, scalex float8, scaley float8);
DROP FUNCTION IF EXISTS st_setskew (rast raster, skew float8);
DROP FUNCTION IF EXISTS st_setskew (rast raster, skewx float8, skewy float8);
DROP FUNCTION IF EXISTS st_setsrid (rast raster, srid integer);
DROP FUNCTION IF EXISTS st_setupperleft (rast raster, upperleftx float8, upperlefty float8);
DROP FUNCTION IF EXISTS st_setrotation (rast raster, rotation float8);
DROP FUNCTION IF EXISTS st_setgeotransform (rast raster,
    imag double precision, 
    jmag double precision,
    theta_i double precision,
    theta_ij double precision,
    xoffset double precision,
    yoffset double precision);
DROP FUNCTION IF EXISTS st_setgeoreference (rast raster, georef text, format text );
DROP FUNCTION IF EXISTS st_setbandnodatavalue (rast raster, band integer, nodatavalue float8, forceChecking boolean );
DROP FUNCTION IF EXISTS st_setbandnodatavalue (rast raster, nodatavalue float8);
DROP FUNCTION IF EXISTS st_setbandisnodata (rast raster, band integer );
DROP FUNCTION IF EXISTS st_setvalue (rast raster, band integer, x integer, y integer, newvalue float8);
DROP FUNCTION IF EXISTS st_setvalue (rast raster, x integer, y integer, newvalue float8);
DROP FUNCTION IF EXISTS st_setvalue (rast raster, band integer, pt geometry, newvalue float8);
DROP FUNCTION IF EXISTS st_setvalue (rast raster, pt geometry, newvalue float8);
DROP FUNCTION IF EXISTS st_dumpaspolygons (rast raster, band integer );
DROP FUNCTION IF EXISTS st_polygon (rast raster, band integer );
DROP FUNCTION IF EXISTS st_pixelaspolygon (rast raster, x integer, y integer);
DROP FUNCTION IF EXISTS ST_PixelAsPolygons (rast raster, band integer , OUT geom geometry, OUT val double precision, OUT x int, OUT y int);
DROP FUNCTION IF EXISTS _st_world2rastercoord (
	rast raster,
	longitude double precision , latitude double precision ,
	OUT columnx integer,
	OUT rowy integer
);
DROP FUNCTION IF EXISTS st_world2rastercoordx (rast raster, xw float8, yw float8);
DROP FUNCTION IF EXISTS st_world2rastercoordx (rast raster, xw float8);
DROP FUNCTION IF EXISTS st_world2rastercoordx (rast raster, pt geometry);
DROP FUNCTION IF EXISTS st_world2rastercoordy (rast raster, xw float8, yw float8);
DROP FUNCTION IF EXISTS st_world2rastercoordy (rast raster, yw float8);
DROP FUNCTION IF EXISTS st_world2rastercoordy (rast raster, pt geometry);
DROP FUNCTION IF EXISTS _st_raster2worldcoord (
	rast raster,
	columnx integer , rowy integer ,
	OUT longitude double precision,
	OUT latitude double precision
);
DROP FUNCTION IF EXISTS st_raster2worldcoordx (rast raster, xr int, yr int);
DROP FUNCTION IF EXISTS st_raster2worldcoordx (rast raster, xr int);
DROP FUNCTION IF EXISTS st_raster2worldcoordy (rast raster, xr int, yr int);
DROP FUNCTION IF EXISTS st_raster2worldcoordy (rast raster, yr int);
DROP FUNCTION IF EXISTS st_minpossiblevalue (pixeltype text);
DROP FUNCTION IF EXISTS st_asbinary (raster);
DROP FUNCTION IF EXISTS bytea (raster);
DROP FUNCTION IF EXISTS raster_overleft (raster, raster);
DROP FUNCTION IF EXISTS raster_overright (raster, raster);
DROP FUNCTION IF EXISTS raster_left (raster, raster);
DROP FUNCTION IF EXISTS raster_right (raster, raster);
DROP FUNCTION IF EXISTS raster_overabove (raster, raster);
DROP FUNCTION IF EXISTS raster_overbelow (raster, raster);
DROP FUNCTION IF EXISTS raster_above (raster, raster);
DROP FUNCTION IF EXISTS raster_below (raster, raster);
DROP FUNCTION IF EXISTS raster_same (raster, raster);
DROP FUNCTION IF EXISTS raster_contained (raster, raster);
DROP FUNCTION IF EXISTS raster_contain (raster, raster);
DROP FUNCTION IF EXISTS raster_overlap (raster, raster);
DROP FUNCTION IF EXISTS raster_geometry_contain (raster, geometry);
DROP FUNCTION IF EXISTS raster_geometry_overlap (raster, geometry);
DROP FUNCTION IF EXISTS geometry_raster_contain (geometry, raster);
DROP FUNCTION IF EXISTS geometry_raster_overlap (geometry, raster);
DROP FUNCTION IF EXISTS st_samealignment (rast1 raster, rast2 raster);
DROP FUNCTION IF EXISTS st_samealignment (
	ulx1 double precision, uly1 double precision, scalex1 double precision, scaley1 double precision, skewx1 double precision, skewy1 double precision,
	ulx2 double precision, uly2 double precision, scalex2 double precision, scaley2 double precision, skewx2 double precision, skewy2 double precision
);
DROP FUNCTION IF EXISTS _st_intersects (rast1 raster, nband1 integer, rast2 raster, nband2 integer);
DROP FUNCTION IF EXISTS st_intersects (rast1 raster, nband1 integer, rast2 raster, nband2 integer);
DROP FUNCTION IF EXISTS st_intersects (rast1 raster, rast2 raster);
DROP FUNCTION IF EXISTS _st_intersects (rast raster, geom geometry, nband integer );
DROP FUNCTION IF EXISTS st_intersects (rast raster, geom geometry, nband integer );
DROP FUNCTION IF EXISTS st_intersects (rast raster, nband integer, geom geometry);
DROP FUNCTION IF EXISTS _st_intersects (geom geometry, rast raster, nband integer );
DROP FUNCTION IF EXISTS st_intersects (geom geometry, rast raster, nband integer );
DROP FUNCTION IF EXISTS st_intersection (geomin geometry, rast raster, band integer );
DROP FUNCTION IF EXISTS st_intersection (rast raster, band integer, geomin geometry);
DROP FUNCTION IF EXISTS st_intersection (rast raster, geomin geometry);
DROP FUNCTION IF EXISTS st_intersection (
	rast1 raster, band1 int,
	rast2 raster, band2 int,
	returnband text ,
	nodataval double precision[] );
DROP FUNCTION IF EXISTS st_intersection (
	rast1 raster, band1 int,
	rast2 raster, band2 int,
	returnband text,
	nodataval double precision
);
DROP FUNCTION IF EXISTS st_intersection (
	rast1 raster, band1 int,
	rast2 raster, band2 int,
	nodataval double precision[]
);
DROP FUNCTION IF EXISTS st_intersection (
	rast1 raster, band1 int,
	rast2 raster, band2 int,
	nodataval double precision
);
DROP FUNCTION IF EXISTS st_intersection (
	rast1 raster,
	rast2 raster,
	returnband text ,
	nodataval double precision[] );
DROP FUNCTION IF EXISTS st_intersection (
	rast1 raster,
	rast2 raster,
	returnband text,
	nodataval double precision
);
DROP FUNCTION IF EXISTS st_intersection (
	rast1 raster,
	rast2 raster,
	nodataval double precision[]
);
DROP FUNCTION IF EXISTS st_intersection (
	rast1 raster,
	rast2 raster,
	nodataval double precision
);
DROP FUNCTION IF EXISTS _ST_MapAlgebra4UnionState (rast1 raster,  rast2 raster, p_expression text, p_nodata1expr text, p_nodata2expr text, p_nodatanodataval double precision,t_expression text,t_nodata1expr text, t_nodata2expr text,t_nodatanodataval double precision);
DROP FUNCTION IF EXISTS _ST_MapAlgebra4UnionState (rast1 raster,rast2 raster,bandnum integer, p_expression text);
DROP FUNCTION IF EXISTS _ST_MapAlgebra4UnionState (rast1 raster,rast2 raster, bandnum integer);
DROP FUNCTION IF EXISTS _ST_MapAlgebra4UnionState (rast1 raster,rast2 raster);
DROP FUNCTION IF EXISTS _ST_MapAlgebra4UnionState (rast1 raster,rast2 raster, p_expression text);
DROP FUNCTION IF EXISTS _ST_MapAlgebra4UnionFinal1 (rast raster);
DROP FUNCTION IF EXISTS st_clip (rast raster, band int, geom geometry, nodataval double precision[] , crop boolean );
DROP FUNCTION IF EXISTS st_clip (rast raster, band int, geom geometry, nodataval double precision, crop boolean );
DROP FUNCTION IF EXISTS st_clip (rast raster, band int, geom geometry, crop boolean);
DROP FUNCTION IF EXISTS st_clip (rast raster, geom geometry, nodataval double precision[] , crop boolean );
DROP FUNCTION IF EXISTS st_clip (rast raster, geom geometry, nodataval double precision, crop boolean );
DROP FUNCTION IF EXISTS st_clip (rast raster, geom geometry, crop boolean);
DROP FUNCTION IF EXISTS _add_raster_constraint (cn name, sql text);
DROP FUNCTION IF EXISTS _drop_raster_constraint (rastschema name, rasttable name, cn name);
DROP FUNCTION IF EXISTS _raster_constraint_info_srid (rastschema name, rasttable name, rastcolumn name);
DROP FUNCTION IF EXISTS _add_raster_constraint_srid (rastschema name, rasttable name, rastcolumn name);
DROP FUNCTION IF EXISTS _drop_raster_constraint_srid (rastschema name, rasttable name, rastcolumn name);
DROP FUNCTION IF EXISTS _raster_constraint_info_scale (rastschema name, rasttable name, rastcolumn name, axis char);
DROP FUNCTION IF EXISTS _add_raster_constraint_scale (rastschema name, rasttable name, rastcolumn name, axis char);
DROP FUNCTION IF EXISTS _drop_raster_constraint_scale (rastschema name, rasttable name, rastcolumn name, axis char);
DROP FUNCTION IF EXISTS _raster_constraint_info_blocksize (rastschema name, rasttable name, rastcolumn name, axis text);
DROP FUNCTION IF EXISTS _add_raster_constraint_blocksize (rastschema name, rasttable name, rastcolumn name, axis text);
DROP FUNCTION IF EXISTS _drop_raster_constraint_blocksize (rastschema name, rasttable name, rastcolumn name, axis text);
DROP FUNCTION IF EXISTS _raster_constraint_info_extent (rastschema name, rasttable name, rastcolumn name);
DROP FUNCTION IF EXISTS _add_raster_constraint_extent (rastschema name, rasttable name, rastcolumn name);
DROP FUNCTION IF EXISTS _drop_raster_constraint_extent (rastschema name, rasttable name, rastcolumn name);
DROP FUNCTION IF EXISTS _raster_constraint_info_alignment (rastschema name, rasttable name, rastcolumn name);
DROP FUNCTION IF EXISTS _add_raster_constraint_alignment (rastschema name, rasttable name, rastcolumn name);
DROP FUNCTION IF EXISTS _drop_raster_constraint_alignment (rastschema name, rasttable name, rastcolumn name);
DROP FUNCTION IF EXISTS _raster_constraint_info_regular_blocking (rastschema name, rasttable name, rastcolumn name);
DROP FUNCTION IF EXISTS _add_raster_constraint_regular_blocking (rastschema name, rasttable name, rastcolumn name);
DROP FUNCTION IF EXISTS _drop_raster_constraint_regular_blocking (rastschema name, rasttable name, rastcolumn name);
DROP FUNCTION IF EXISTS _raster_constraint_info_num_bands (rastschema name, rasttable name, rastcolumn name);
DROP FUNCTION IF EXISTS _add_raster_constraint_num_bands (rastschema name, rasttable name, rastcolumn name);
DROP FUNCTION IF EXISTS _drop_raster_constraint_num_bands (rastschema name, rasttable name, rastcolumn name);
DROP FUNCTION IF EXISTS _raster_constraint_info_pixel_types (rastschema name, rasttable name, rastcolumn name);
DROP FUNCTION IF EXISTS _raster_constraint_pixel_types (rast raster);
DROP FUNCTION IF EXISTS _add_raster_constraint_pixel_types (rastschema name, rasttable name, rastcolumn name);
DROP FUNCTION IF EXISTS _drop_raster_constraint_pixel_types (rastschema name, rasttable name, rastcolumn name);
DROP FUNCTION IF EXISTS _raster_constraint_info_nodata_values (rastschema name, rasttable name, rastcolumn name);
DROP FUNCTION IF EXISTS _raster_constraint_nodata_values (rast raster);
DROP FUNCTION IF EXISTS _add_raster_constraint_nodata_values (rastschema name, rasttable name, rastcolumn name);
DROP FUNCTION IF EXISTS _drop_raster_constraint_nodata_values (rastschema name, rasttable name, rastcolumn name);
DROP FUNCTION IF EXISTS _raster_constraint_info_out_db (rastschema name, rasttable name, rastcolumn name);
DROP FUNCTION IF EXISTS _raster_constraint_out_db (rast raster);
DROP FUNCTION IF EXISTS _add_raster_constraint_out_db (rastschema name, rasttable name, rastcolumn name);
DROP FUNCTION IF EXISTS _drop_raster_constraint_out_db (rastschema name, rasttable name, rastcolumn name);
DROP FUNCTION IF EXISTS AddRasterConstraints  (
	rastschema name,
	rasttable name,
	rastcolumn name,
	VARIADIC constraints text[]
);
DROP FUNCTION IF EXISTS AddRasterConstraints  (
	rasttable name,
	rastcolumn name,
	VARIADIC constraints text[]
);
DROP FUNCTION IF EXISTS AddRasterConstraints  (
	rastschema name,
	rasttable name,
	rastcolumn name,
	srid boolean ,
	scale_x boolean ,
	scale_y boolean ,
	blocksize_x boolean ,
	blocksize_y boolean ,
	same_alignment boolean ,
	regular_blocking boolean , -- false as regular_blocking is not a usable constraint
	num_bands boolean ,
	pixel_types boolean ,
	nodata_values boolean ,
	out_db boolean ,
	extent boolean );
DROP FUNCTION IF EXISTS AddRasterConstraints  (
	rasttable name,
	rastcolumn name,
	srid boolean ,
	scale_x boolean ,
	scale_y boolean ,
	blocksize_x boolean ,
	blocksize_y boolean ,
	same_alignment boolean ,
	regular_blocking boolean , -- false as regular_blocking is not a usable constraint
	num_bands boolean ,
	pixel_types boolean ,
	nodata_values boolean ,
	out_db boolean ,
	extent boolean );
DROP FUNCTION IF EXISTS DropRasterConstraints  (
	rastschema name,
	rasttable name,
	rastcolumn name,
	VARIADIC constraints text[]
);
DROP FUNCTION IF EXISTS DropRasterConstraints  (
	rasttable name,
	rastcolumn name,
	VARIADIC constraints text[]
);
DROP FUNCTION IF EXISTS DropRasterConstraints  (
	rastschema name,
	rasttable name,
	rastcolumn name,
	srid boolean ,
	scale_x boolean ,
	scale_y boolean ,
	blocksize_x boolean ,
	blocksize_y boolean ,
	same_alignment boolean ,
	regular_blocking boolean ,
	num_bands boolean ,
	pixel_types boolean ,
	nodata_values boolean ,
	out_db boolean ,
	extent boolean );
DROP FUNCTION IF EXISTS DropRasterConstraints  (
	rasttable name,
	rastcolumn name,
	srid boolean ,
	scale_x boolean ,
	scale_y boolean ,
	blocksize_x boolean ,
	blocksize_y boolean ,
	same_alignment boolean ,
	regular_blocking boolean ,
	num_bands boolean ,
	pixel_types boolean ,
	nodata_values boolean ,
	out_db boolean ,
	extent boolean );
DROP FUNCTION IF EXISTS _overview_constraint (ov raster, factor integer, refschema name, reftable name, refcolumn name);
DROP FUNCTION IF EXISTS _overview_constraint_info (
	ovschema name, ovtable name, ovcolumn name,
	OUT refschema name, OUT reftable name, OUT refcolumn name, OUT factor integer
);
DROP FUNCTION IF EXISTS _add_overview_constraint (
	ovschema name, ovtable name, ovcolumn name,
	refschema name, reftable name, refcolumn name,
	factor integer
);
DROP FUNCTION IF EXISTS _drop_overview_constraint (ovschema name, ovtable name, ovcolumn name);
DROP FUNCTION IF EXISTS AddOverviewConstraints  (
	ovschema name, ovtable name, ovcolumn name,
	refschema name, reftable name, refcolumn name,
	ovfactor int
);
DROP FUNCTION IF EXISTS AddOverviewConstraints  (
	ovtable name, ovcolumn name,
	reftable name, refcolumn name,
	ovfactor int
);
DROP FUNCTION IF EXISTS DropOverviewConstraints  (
	ovschema name,
	ovtable name,
	ovcolumn name
);
DROP FUNCTION IF EXISTS DropOverviewConstraints  (
	ovtable name,
	ovcolumn name
);
-- Drop all types.
DROP TYPE raster CASCADE;
DROP TYPE summarystats CASCADE;
DROP TYPE histogram CASCADE;
DROP TYPE quantile CASCADE;
DROP TYPE valuecount CASCADE;
DROP TYPE reclassarg CASCADE;
DROP TYPE geomval CASCADE;
-- Drop all functions needed for types definition.
-- Drop all schemas.

COMMIT;
