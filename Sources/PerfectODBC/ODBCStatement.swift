//
//  File.swift
//  
//
//  Created by Kyle Jessup on 2020-01-03.
//

import Foundation
import unixodbc

public class ODBCStatement: ODBCHandle {
	public enum FetchResult {
		case success, noData, stillExecuting
	}
	public enum MoreResult {
		case success, noData, stillExecuting, paramDataAvailable
	}
	public struct ColumnDescription {
		let number: Int
		let name: String
		let type: ODBCColumnType
		let size: Int
		let digits: Int
		let nullable: Bool
	}
	var hdbc: SQLHDBC? { connection.hdbc }
	var henv: SQLHENV? { connection.environment.henv }
	var hstmt: SQLHSTMT? { super.handle as SQLHSTMT? }
	public let connection: ODBCConnection
	init(con: ODBCConnection) throws {
		self.connection = con
		super.init(type: SQL_HANDLE_STMT)
		try connection.check(SQLAllocHandle(type, hdbc, &handle))
	}
	public func cancel() throws {
		try connection.check(SQLCancel(hstmt))
	}
	public func execute() throws {
		try connection.check(SQLExecute(hstmt))
	}
	public func rowCount() throws -> Int {
		var count: SQLLEN = 0
		try connection.check(SQLRowCount(hstmt, &count))
		return Int(count)
	}
	public func fetch() throws -> FetchResult {
		let rc = SQLFetch(hstmt)
		switch Int32(rc) {
		case SQL_SUCCESS, SQL_SUCCESS_WITH_INFO:
			return .success
		case SQL_NO_DATA:
			return .noData
		case SQL_STILL_EXECUTING:
			return .stillExecuting
		default:
			try connection.check(rc) // will throw
			return .success
		}
	}
	public func moreResults() throws -> MoreResult {
		let rc = SQLFetch(hstmt)
		switch Int32(rc) {
		case SQL_SUCCESS, SQL_SUCCESS_WITH_INFO:
			return .success
		case SQL_NO_DATA:
			return .noData
		case SQL_STILL_EXECUTING:
			return .stillExecuting
		case SQL_PARAM_DATA_AVAILABLE:
			return .paramDataAvailable
		default:
			try connection.check(rc) // will throw
			return .success
		}
	}
	public func numResultColumns() throws -> Int {
		var num: SQLSMALLINT = 0
		try connection.check(SQLNumResultCols(hstmt, &num))
		return Int(num)
	}
	// 1 based
	public func describeColumn(number: Int) throws -> ColumnDescription {
		let name = UnsafeMutableBufferPointer<SQLCHAR>.allocate(capacity: bigNameLen)
		defer {
			name.deallocate()
		}
		var nameLen: SQLSMALLINT = 0
		var dataType: SQLSMALLINT = 0
		var columnSize: SQLULEN = 0
		var decimalDigits: SQLSMALLINT = 0
		var nullable: SQLSMALLINT = 0
		try connection.check(SQLDescribeCol(hstmt,
											SQLUSMALLINT(number),
											name.baseAddress,
											SQLSMALLINT(bigNameLen),
											&nameLen,
											&dataType,
											&columnSize,
											&decimalDigits,
											&nullable))
		return .init(number: number,
					 name: String(bytes: name[0..<Int(nameLen)], encoding: .utf8) ?? "",
					 type: .init(rawValue: dataType),
					 size: Int(columnSize),
					 digits: Int(decimalDigits),
					 nullable: nullable == SQL_NULLABLE)
	}
}
