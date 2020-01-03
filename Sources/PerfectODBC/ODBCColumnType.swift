//
//  ODBCColumnType.swift
//  
//
//  Created by Kyle Jessup on 2020-01-03.
//

import Foundation
import unixodbc

public enum ODBCColumnType: RawRepresentable {
	case char, varchar, longvarchar, wchar, wvarchar, wlongvarchar, decimal, numeric, smallint,
		integer, real, float, double, bit, tinyint, bigint, binary, varbinary, longvarbinary,
		date, time, timestamp, intervalmonth, intervalyear, intervalyeartomonth,
		intervalday, intervalhour, intervalminute, intervalsecond, intervaldaytohour,
		intervaldaytominute, intervaldaytosecond, intervalhourtominute,
		intervalhourtosecond, intervalminutetosecond, guid
	case unknown
	
	public typealias RawValue = Int16
	public init(rawValue: Int16) {
		switch rawValue {
			case _SQL_CHAR: self = .char
			case _SQL_VARCHAR: self = .varchar
			case _SQL_LONGVARCHAR: self = .longvarchar
			case _SQL_WCHAR: self = .wchar
			case _SQL_WVARCHAR: self = .wvarchar
			case _SQL_WLONGVARCHAR: self = .wlongvarchar
			case _SQL_DECIMAL: self = .decimal
			case _SQL_NUMERIC: self = .numeric
			case _SQL_SMALLINT: self = .smallint
			case _SQL_INTEGER: self = .integer
			case _SQL_REAL: self = .real
			case _SQL_FLOAT: self = .float
			case _SQL_DOUBLE: self = .double
			case _SQL_BIT: self = .bit
			case _SQL_TINYINT: self = .tinyint
			case _SQL_BIGINT: self = .bigint
			case _SQL_BINARY: self = .binary
			case _SQL_VARBINARY: self = .varbinary
			case _SQL_LONGVARBINARY: self = .longvarbinary
			case _SQL_TYPE_DATE: self = .date
			case _SQL_TYPE_TIME: self = .time
			case _SQL_TYPE_TIMESTAMP: self = .timestamp
			case _SQL_INTERVAL_MONTH: self = .intervalmonth
			case _SQL_INTERVAL_YEAR: self = .intervalyear
			case _SQL_INTERVAL_YEAR_TO_MONTH: self = .intervalyeartomonth
			case _SQL_INTERVAL_DAY: self = .intervalday
			case _SQL_INTERVAL_HOUR: self = .intervalhour
			case _SQL_INTERVAL_MINUTE: self = .intervalminute
			case _SQL_INTERVAL_SECOND: self = .intervalsecond
			case _SQL_INTERVAL_DAY_TO_HOUR: self = .intervaldaytohour
			case _SQL_INTERVAL_DAY_TO_MINUTE: self = .intervaldaytominute
			case _SQL_INTERVAL_DAY_TO_SECOND: self = .intervaldaytosecond
			case _SQL_INTERVAL_HOUR_TO_MINUTE: self = .intervalhourtominute
			case _SQL_INTERVAL_HOUR_TO_SECOND: self = .intervalhourtosecond
			case _SQL_INTERVAL_MINUTE_TO_SECOND: self = .intervalminutetosecond
			case _SQL_GUID: self = .guid
			default: self = .unknown
		}
	}
	public var rawValue: RawValue {
		switch self {
			case .char: return _SQL_CHAR
			case .varchar: return _SQL_VARCHAR
			case .longvarchar: return _SQL_LONGVARCHAR
			case .wchar: return _SQL_WCHAR
			case .wvarchar: return _SQL_WVARCHAR
			case .wlongvarchar: return _SQL_WLONGVARCHAR
			case .decimal: return _SQL_DECIMAL
			case .numeric: return _SQL_NUMERIC
			case .smallint: return _SQL_SMALLINT
			case .integer: return _SQL_INTEGER
			case .real: return _SQL_REAL
			case .float: return _SQL_FLOAT
			case .double: return _SQL_DOUBLE
			case .bit: return _SQL_BIT
			case .tinyint: return _SQL_TINYINT
			case .bigint: return _SQL_BIGINT
			case .binary: return _SQL_BINARY
			case .varbinary: return _SQL_VARBINARY
			case .longvarbinary: return _SQL_LONGVARBINARY
			case .date: return _SQL_TYPE_DATE
			case .time: return _SQL_TYPE_TIME
			case .timestamp: return _SQL_TYPE_TIMESTAMP
			case .intervalmonth: return _SQL_INTERVAL_MONTH
			case .intervalyear: return _SQL_INTERVAL_YEAR
			case .intervalyeartomonth: return _SQL_INTERVAL_YEAR_TO_MONTH
			case .intervalday: return _SQL_INTERVAL_DAY
			case .intervalhour: return _SQL_INTERVAL_HOUR
			case .intervalminute: return _SQL_INTERVAL_MINUTE
			case .intervalsecond: return _SQL_INTERVAL_SECOND
			case .intervaldaytohour: return _SQL_INTERVAL_DAY_TO_HOUR
			case .intervaldaytominute: return _SQL_INTERVAL_DAY_TO_MINUTE
			case .intervaldaytosecond: return _SQL_INTERVAL_DAY_TO_SECOND
			case .intervalhourtominute: return _SQL_INTERVAL_HOUR_TO_MINUTE
			case .intervalhourtosecond: return _SQL_INTERVAL_HOUR_TO_SECOND
			case .intervalminutetosecond: return _SQL_INTERVAL_MINUTE_TO_SECOND
			case .guid: return _SQL_GUID
			case .unknown: return _SQL_UNKNOWN_TYPE
		}
	}
}
