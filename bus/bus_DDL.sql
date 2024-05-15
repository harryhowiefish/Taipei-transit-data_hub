CREATE TABLE `bus_station` (
  `bus_station_id` int PRIMARY KEY,
  `station_name` varchar(20),
  `address` varchar(50),
  `city_code` char(3),
  `district` varchar(10),
  `lat` decimal(8,6),
  `lng` decimal(9,6),
  `bearing` char(1),
  `create_time` timestamp DEFAULT CURRENT_TIMESTAMP,
  `update_time` timestamp DEFAULT CURRENT_TIMESTAMP
);