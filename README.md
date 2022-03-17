# NC_Gen
Create natural cities from massive points of shapefile
Replace the inputurl and outputurl with your own data to create natural cities automatically.
THe addin supports ArcGIS 10.2, installation of ArcObjects for .NET is a prerequisite.

The term ‘natural cities’ refers to the human settlements or human activities in general on Earth’s surface that are naturally or objectively defined and delineated from massive geographic information based on head/tail division rule, the non-recursive form of head/tail breaks (Jiang and Miao 2015, Jiang 2015). Such geographic information comes from various sources, such as massive street junctions and street ends, a massive number of street blocks, nighttime imagery and social media users’ locations. Distinct from conventional cities, the natural cities are objectively and naturally derived. They are created from a mean value calculated from a large amount of units extracted from geographic information (Jiang and Miao 2015). For example, these units can be area units for the street blocks and pixels for the nighttime images. This model is created by ArcGIS model builder based on OpenStreetMap (OSM) data and it consists of two parts: (1) extracting street nodes from OSM data and (2) creating natural cities from the street nodes. It can be applied to an entire country OSM data involving millions of street nodes, or thoudsands of natural cities.

References
Jiang B., Jia T.(2011), Zipf's law for all the natural cities in the United States: a geospatial perspective, International Journal of Geographical Information Science, 25, 8.
Jiang B. (2015). "Head/tail breaks for visualization of city structure and dynamics", Cities, 43, 69 - 77.
Jiang B. and Miao Y. (2015). "The evolution of natural cities from the perspective of location-based social media", The Professional Geographer, 67(2), 295 - 306.
