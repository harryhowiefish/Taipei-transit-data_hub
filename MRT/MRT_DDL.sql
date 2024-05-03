create database `group2_db`;
use `group2_db`;
CREATE TABLE `mrt_realtime_arrival` (
  `mrt_station_name` varchar(10),
  `destination_name` varchar(10),
  `arrive_time` int,
  `update_time` timestamp
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

CREATE TABLE `mrt_parking` (
  `park_name` varchar(20),
  `mrt_station_id` varchar(5),
  `mrt_station_name` varchar(10),
  `line_type` varchar(5),
  `parking_type`char(2),
  `available_space` int,
  `total_space` int,
  `update_time` timestamp

);


