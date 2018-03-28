
/*
 * Loads all ways in an OSM XML file. Returns one dataset that conatins:
 * - segment_id: A generated ID which is unique for each segment
 * - id1: ID of the first node
 * - latitude1: Latitude of the first node
 * - longitude1: Longitude of the first node
 * - id2: ID of the second node
 * - latitude2: Latitude of the second node
 * - longitude2: Longitude of the second node
 * - way_id: The ID of the way which contains this segment
 * - tags: Tags of the way that contains this segment
 */

REGISTER spatialhadoop-2.3.jar;
REGISTER pigeon-0.2.0.jar;
REGISTER esri-geometry-api-1.2.jar;
REGISTER jts-1.8.jar;
REGISTER piggybank.jar;

IMPORT 'pigeon_import.pig';

/*
 * Reads all OSM nodes from an XML file and return one dataset that contains
 * - node_id: ID of the node as appears in XML
 * - latitude:
 * - longitude:
 * - tags: A map of tags as appear in the XML file
 */
DEFINE LoadOSMNodes(osm_file) RETURNS nodes {
  xml_nodes = LOAD '$osm_file' USING org.apache.pig.piggybank.storage.XMLLoader('node') AS (node:chararray);
  osm_nodes = FOREACH xml_nodes GENERATE edu.umn.cs.spatialHadoop.osm.OSMNode(node) AS node;
  $nodes = FOREACH osm_nodes
    GENERATE node.id AS osm_node_id, node.lon AS longitude, node.lat AS latitude, node.tags AS tags;
};

  xml_ways = LOAD '/home/mugdha/Downloads/us-south-latest.osm.bz2' USING           org.apache.pig.piggybank.storage.XMLLoader('way') AS (way:chararray);
  osm_ways = FOREACH xml_ways GENERATE edu.umn.cs.spatialHadoop.osm.OSMWay(way) AS way;

  -- Project columns of interest in ways and flatten nodes
  osm_ways = FOREACH osm_ways
    GENERATE way.id AS way_id, FLATTEN(way.nodes), way.tags AS tags;

  -- Project node ID and point location from nodes
  
  osm_nodes = LoadOSMNodes('/home/mugdha/Downloads/us-south-latest.osm.bz2');
  node_locations = FOREACH osm_nodes
    GENERATE osm_node_id, ST_MakePoint(longitude, latitude) AS location;

  -- Join ways with nodes to find the location of each node (lat, lon)
  joined_ways = JOIN node_locations BY osm_node_id, osm_ways BY nodes::node_id PARALLEL 70;
  
  -- Group all node locations of each way by way ID
  ways_with_nodes = GROUP joined_ways BY way_id PARALLEL 70;
  
  way_segments = FOREACH ways_with_nodes {
    -- order points by position
    ordered = ORDER joined_ways BY pos;
    -- All tags are similar. Just grab the first one
    tags = FOREACH joined_ways GENERATE tags;
    GENERATE group AS way_id, ST_MakeSegments(ordered.osm_node_id, ordered.location) AS segments,
      FLATTEN(TOP(1, 0, tags)) AS tags;
  };
  
  way_segments = FOREACH way_segments GENERATE way_id, FLATTEN(segments), tags;
  
  ways_with_segments = FOREACH way_segments
    GENERATE CONCAT((CHARARRAY)way_id, (CHARARRAY)position) AS segment_id,
    id1, x1 AS longitude1, y1 AS latitude1,
    id2, x2 AS longitude2, y2 AS latitude2, way_id, tags;

STORE ways_with_segments into '$output/all_ways_south_latest.bz2' USING PigStorage(',');
