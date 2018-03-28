REGISTER pigeon.jar;
REGISTER jts-1.8.jar;
REGISTER esri-geometry-api-1.2.jar;

IMPORT 'pigeon_import.pig';

polys = LOAD '/home/mugdha/Documents/Osmix/county.shp' Using PigStorage() AS (geom1);

polys = FOREACH polys GENERATE FLATTEN(ST_Decompose(geom1, 100)) AS geom1;
polys = FOREACH polys GENERATE ST_AsText(geom1);
STORE polys INTO '/home/mugdha/Documents/Osmix/output';
