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
		let type: ODBCDataType
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
		try check(SQLAllocHandle(type, hdbc, &handle))
	}
	public func cancel() throws {
		try check(SQLCancel(hstmt))
	}
	public func execute() throws {
		try check(SQLExecute(hstmt))
	}
	public func rowCount() throws -> Int {
		var count: SQLLEN = 0
		try check(SQLRowCount(hstmt, &count))
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
			try check(rc) // will throw
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
			try check(rc) // will throw
			return .success
		}
	}
	public func numResultColumns() throws -> Int {
		var num: SQLSMALLINT = 0
		try check(SQLNumResultCols(hstmt, &num))
		return Int(num)
	}
	public func numParams() throws -> Int {
		var num: SQLSMALLINT = 0
		try check(SQLNumParams(hstmt, &num))
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
		try check(SQLDescribeCol(hstmt,
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
	public func getData(number: Int, buffer: UnsafeMutableRawBufferPointer, type: ODBCDataType = .cdefault) throws -> Int? {
		var len: SQLLEN = 0
		try check(SQLGetData(hstmt, SQLUSMALLINT(number), type.rawValue, buffer.baseAddress, buffer.count, &len))
		guard len != SQL_NULL_DATA else {
			return nil
		}
		return Int(len)
	}
	public func getData(number: Int, estimatedSize: Int = 256, encoding: String.Encoding = .utf8) throws -> String? {
		var firstBuff = UnsafeMutableRawBufferPointer.allocate(byteCount: estimatedSize, alignment: 0)
		defer {
			firstBuff.deallocate()
		}
		guard let fullLen = try getData(number: number, buffer: firstBuff, type: .char) else {
			return nil
		}
		if fullLen > estimatedSize {
			let nextBuf = UnsafeMutableRawBufferPointer.allocate(byteCount: fullLen, alignment: 0)
			nextBuf.copyMemory(from: UnsafeRawBufferPointer(firstBuff))
			firstBuff.deallocate()
			firstBuff = nextBuf
			var len: SQLLEN = 0
			try check(SQLGetData(hstmt, SQLUSMALLINT(number), ODBCDataType.char.rawValue, firstBuff.baseAddress!.advanced(by: estimatedSize-1), fullLen, &len))
		}
		return String(bytes: firstBuff[0..<fullLen], encoding: encoding)
	}
	public func getData(number: Int) throws -> Double? {
		var len: SQLLEN = 0
		var val: Double = 0.0
		try check(SQLGetData(hstmt, SQLUSMALLINT(number), ODBCDataType.double.rawValue, &val, MemoryLayout.size(ofValue: val), &len))
		guard len != SQL_NULL_DATA else {
			return nil
		}
		return val
	}
	public func getData(number: Int) throws -> Float? {
		var len: SQLLEN = 0
		var val: Float = 0.0
		try check(SQLGetData(hstmt, SQLUSMALLINT(number), ODBCDataType.float.rawValue, &val, MemoryLayout.size(ofValue: val), &len))
		guard len != SQL_NULL_DATA else {
			return nil
		}
		return val
	}
	
	public func getData(number: Int) throws -> Int8? {
		var len: SQLLEN = 0
		var val: Int8 = 0
		try check(SQLGetData(hstmt, SQLUSMALLINT(number), ODBCDataType.cstinyint.rawValue, &val, MemoryLayout.size(ofValue: val), &len))
		guard len != SQL_NULL_DATA else {
			return nil
		}
		return val
	}
	public func getData(number: Int) throws -> UInt8? {
		var len: SQLLEN = 0
		var val: UInt8 = 0
		try check(SQLGetData(hstmt, SQLUSMALLINT(number), ODBCDataType.cutinyint.rawValue, &val, MemoryLayout.size(ofValue: val), &len))
		guard len != SQL_NULL_DATA else {
			return nil
		}
		return val
	}
	
	public func getData(number: Int) throws -> Int16? {
		var len: SQLLEN = 0
		var val: Int16 = 0
		try check(SQLGetData(hstmt, SQLUSMALLINT(number), ODBCDataType.csshort.rawValue, &val, MemoryLayout.size(ofValue: val), &len))
		guard len != SQL_NULL_DATA else {
			return nil
		}
		return val
	}
	public func getData(number: Int) throws -> UInt16? {
		var len: SQLLEN = 0
		var val: UInt16 = 0
		try check(SQLGetData(hstmt, SQLUSMALLINT(number), ODBCDataType.cushort.rawValue, &val, MemoryLayout.size(ofValue: val), &len))
		guard len != SQL_NULL_DATA else {
			return nil
		}
		return val
	}
	
	public func getData(number: Int) throws -> Int32? {
		var len: SQLLEN = 0
		var val: Int32 = 0
		try check(SQLGetData(hstmt, SQLUSMALLINT(number), ODBCDataType.cslong.rawValue, &val, MemoryLayout.size(ofValue: val), &len))
		guard len != SQL_NULL_DATA else {
			return nil
		}
		return val
	}
	public func getData(number: Int) throws -> UInt32? {
		var len: SQLLEN = 0
		var val: UInt32 = 0
		try check(SQLGetData(hstmt, SQLUSMALLINT(number), ODBCDataType.culong.rawValue, &val, MemoryLayout.size(ofValue: val), &len))
		guard len != SQL_NULL_DATA else {
			return nil
		}
		return val
	}
	
	public func getData(number: Int) throws -> Int64? {
		var len: SQLLEN = 0
		var val: Int64 = 0
		try check(SQLGetData(hstmt, SQLUSMALLINT(number), ODBCDataType.csbigint.rawValue, &val, MemoryLayout.size(ofValue: val), &len))
		guard len != SQL_NULL_DATA else {
			return nil
		}
		return val
	}
	public func getData(number: Int) throws -> UInt64? {
		var len: SQLLEN = 0
		var val: UInt64 = 0
		try check(SQLGetData(hstmt, SQLUSMALLINT(number), ODBCDataType.cubigint.rawValue, &val, MemoryLayout.size(ofValue: val), &len))
		guard len != SQL_NULL_DATA else {
			return nil
		}
		return val
	}
	public func getData(number: Int) throws -> Int? {
		guard let r: Int64 = try getData(number: number) else {
			return nil
		}
		return Int(truncatingIfNeeded: r)
	}
	public func getData(number: Int) throws -> UInt? {
		guard let r: UInt64 = try getData(number: number) else {
			return nil
		}
		return UInt(truncatingIfNeeded: r)
	}
	func check(_ code: SQLRETURN) throws {
		guard SQL_SUCCEEDED(code) else {
			let maxMsgSize = bigNameLen
			let sqlState = UnsafeMutableBufferPointer<SQLCHAR>.allocate(capacity: 6)
			let errMsg = UnsafeMutableBufferPointer<SQLCHAR>.allocate(capacity: maxMsgSize)
			defer {
				sqlState.deallocate()
				errMsg.deallocate()
			}
			var errCode: SQLINTEGER = 0
			var errorSize: SQLSMALLINT = 0
			SQLError(henv, hdbc, hstmt,
					 sqlState.baseAddress,
					 &errCode,
					 errMsg.baseAddress,
					 SQLSMALLINT(maxMsgSize),
					 &errorSize)
			let msgStr = "[\(String(bytes: sqlState[0..<5], encoding: .utf8) ?? "")]\(String(bytes: errMsg[0..<Int(errorSize)], encoding: .utf8) ?? "")"
			throw ODBCError.error(msgStr)
		}
	}
}
