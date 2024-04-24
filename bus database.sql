CREATE TABLE `Estimate_arrival_log` (
  `Route_ID` int,
  `Stop_ID` int,
  `Direction` bool,
  `Estimate_time` int,
  `create_time` timestamp
);

CREATE TABLE `Realtime` (
  `PlateNumb` varchar(10),
  `RouteID` int,
  `SubRouteID` int,
  `Direction` int,
  `lon` float,
  `lat` float,
  `NearestStopID` int,
  `speed` int,
  `DutyStatus` bool,
  `BusStatus` bool,
  `create_time` timestamp
);

CREATE TABLE `route` (
  `RouteID` int,
  `OperatorID` int,
  `RouteName` varchar(20),
  `DepartureStopName` varchar(20),
  `DestinationStopName` varchar(20),
  `TicketPrice` varchar(10),
  `CityCode` char(3),
  `create_time` timestamp,
  `update_time` timestamp
);

CREATE TABLE `subRoute` (
  `RouteID` int,
  `SubRouteID` int,
  `SubRouteName` varchar(10),
  `Direction` int,
  `FirstBusTime` char(4),
  `LastBusTime` char(4),
  `HolidayFirstBusTime` char(4),
  `HolidayLastBusTime` char(4),
  `create_time` timestamp,
  `update_time` timestamp
);

CREATE TABLE `Operator` (
  `OperatorID` int,
  `OperatorName` varchar(20),
  `create_time` timestamp,
  `update_time` timestamp
);

CREATE TABLE `RouteStopInfo` (
  `RouteID` int,
  `StopID` int,
  `StopSequence` int,
  `Direction` int,
  `create_time` timestamp,
  `update_time` timestamp
);

CREATE TABLE `Stop` (
  `StopID` int,
  `StationID` int,
  `StopName` varchar(20),
  `create_time` timestamp,
  `update_time` timestamp
);

CREATE TABLE `Station` (
  `StationID` int,
  `StationName` varchar(20),
  `Address` varchar(50),
  `County` varchar(10),
  `lon` float,
  `lng` float,
  `bearing` char(1),
  `CityCode` char(3),
  `create_time` timestamp,
  `update_time` timestamp
);

ALTER TABLE `route` ADD FOREIGN KEY (`OperatorID`) REFERENCES `Operator` (`OperatorID`);

ALTER TABLE `subRoute` ADD FOREIGN KEY (`RouteID`) REFERENCES `route` (`RouteID`);

ALTER TABLE `RouteStopInfo` ADD FOREIGN KEY (`RouteID`) REFERENCES `route` (`RouteID`);

ALTER TABLE `Estimate_arrival_log` ADD FOREIGN KEY (`Route_ID`) REFERENCES `route` (`RouteID`);

ALTER TABLE `Stop` ADD FOREIGN KEY (`StationID`) REFERENCES `Station` (`StationID`);

CREATE TABLE `Stop_RouteStopInfo` (
  `Stop_StopID` int,
  `RouteStopInfo_StopID` int,
  PRIMARY KEY (`Stop_StopID`, `RouteStopInfo_StopID`)
);

ALTER TABLE `Stop_RouteStopInfo` ADD FOREIGN KEY (`Stop_StopID`) REFERENCES `Stop` (`StopID`);

ALTER TABLE `Stop_RouteStopInfo` ADD FOREIGN KEY (`RouteStopInfo_StopID`) REFERENCES `RouteStopInfo` (`StopID`);


ALTER TABLE `Estimate_arrival_log` ADD FOREIGN KEY (`Stop_ID`) REFERENCES `Stop` (`StopID`);

ALTER TABLE `Realtime` ADD FOREIGN KEY (`RouteID`) REFERENCES `route` (`RouteID`);
