create database `group2_db`;
use `group2_db`;
CREATE TABLE `mrt_realtime_arrival` (
  `mrt_station_name` varchar(10),
  `destination_name` varchar(10),
  `arrive_time` int,
  `update_time` timestamp,
  PRIMARY KEY(`mrt_station_name`,`destination_name`,`update_time`)
);

CREATE TABLE `mrt_realtime_crowded` (
  `mrt_station_id` varchar(5),
  `mrt_station_name` varchar(10),
  `line_type` varchar(5),
  `direction` varchar(2),
  `cart1` int,
  `cart2` int,
  `cart3` int,
  `cart4` int,
  `cart5` int,
  `cart6` int,
  `update_time` timestamp,
  PRIMARY KEY (`mrt_station_id`, `line_type`,`direction` ,`update_time`)
);

-- CREATE TABLE `mrt_parking` (
--   `park_name` varchar(20),
--   `mrt_station_id` varchar(5),
--   `mrt_station_name` varchar(10),
--   `line_type` varchar(5),
--   `parking_type`char(2),
--   `available_space` int,
--   `total_space` int,
--   `update_time` timestamp

-- );
CREATE TABLE `mrt_parking_info` (
  `park_name` varchar(20),
  `mrt_station_name` varchar(10),
  `line_type` varchar(5),
  `parking_type`char(2),
  `total_space` int,
  `update_time` timestamp,
  PRIMARY KEY(`park_name`,`update_time`)

);

CREATE TABLE `mrt_realtime_parking` (
  `park_name` varchar(20),
  `available_space` int,
  `update_time` timestamp,
  PRIMARY KEY (`park_name`, `update_time`)
);

CREATE TABLE `mrt_usage_history` (
  `date` date ,
  `hour` int,
  `mrt_station_name_enter` varchar(10),
  `mrt_station_name_exit` varchar(10),
  `visitors_num` int,
  PRIMARY KEY(`date`,`hour`,`mrt_station_name_enter`,`mrt_station_name_exit`)
);
			

CREATE TABLE `mrt_station` (
  `mrt_station_id` int PRIMARY KEY,
  `linetype` varchar(5),
  `mrt_station_name` varchar(20),
  `station_en` varchar(100),
  `station_address` varchar(150),
  `lat` decimal(8,6),
  `lng` decimal(9,6),
  `city_code` char(3),
  `district` varchar(10),
  `bike_allow_on_holiday` bool,
  `update_time` timestamp
);