//
//  File.swift
//  
//
//  Created by Kyle Jessup on 2020-01-03.
//

import Foundation
import unixodbc

extension SQLGUID {
	init(_ uuid: uuid_t) {
		self.init()
		Data1 = (UInt32(uuid.0) << 24) + (UInt32(uuid.1) << 16)
		Data1 += (UInt32(uuid.2) << 8) + UInt32(uuid.3)
		Data2 = (UInt16(uuid.4) << 8) + UInt16(uuid.5)
		Data3 = (UInt16(uuid.6) << 8) + UInt16(uuid.7)
		Data4 = (uuid.8, uuid.9, uuid.10, uuid.11, uuid.12, uuid.13, uuid.14, uuid.15)
	}
}

public class ODBCStatement: ODBCHandle {
	struct BoundParameter {
		let value: Any
		let type: ODBCDataType
	}
	var bindAtExec: [Int:BoundParameter] = [:]
	var _bindValues: UnsafeMutablePointer<SQLLEN>?
	var bindValues: UnsafeMutablePointer<SQLLEN>? {
		if let bv = _bindValues {
			return bv
		}
		let paramCount: Int = try! numParams()
		if paramCount > 0 {
			_bindValues = .allocate(capacity: paramCount)
		}
		return _bindValues
	}
	
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
		try check(SQLAllocHandle(handleType, hdbc, &handle))
	}
	deinit {
		if let bvals = _bindValues {
			bvals.deallocate()
		}
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

extension ODBCStatement {
	public func cancel() throws {
		try check(SQLCancel(hstmt))
	}
	public func numParams() throws -> Int {
		var num: SQLSMALLINT = 0
		try check(SQLNumParams(hstmt, &num))
		return Int(num)
	}
	public func rowCount() throws -> Int {
		var count: SQLLEN = 0
		try check(SQLRowCount(hstmt, &count))
		return Int(count)
	}
	public func closeCursor() throws {
//		try check(SQLCloseCursor(hstmt))
		try check(SQLFreeStmt(hstmt, SQLUSMALLINT(SQL_CLOSE)))
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
}

// internal - user-facing prep & execute go through connection obj
extension ODBCStatement {
	func prepare(statement: String) throws {
		var statement = statement
		try check(statement.withUTF8 {
			s in
			return SQLPrepare(hstmt,
							  UnsafeMutablePointer<UInt8>(mutating: s.baseAddress),
							  SQLINTEGER(s.count))
		})
	}
	func execute(statement: String) throws {
		var statement = statement
		try check(statement.withUTF8 {
			s in
			return SQLExecDirect(hstmt,
							  UnsafeMutablePointer<UInt8>(mutating: s.baseAddress),
							  SQLINTEGER(s.count))
		})
	}
}

extension ODBCStatement {
	public func execute() throws {
		var rc = SQLExecute(hstmt)
		var ptr: SQLPOINTER? = nil
		while rc == SQL_NEED_DATA {
			rc = SQLParamData(hstmt, &ptr)
			if rc == SQL_NEED_DATA {
				guard let ptrBang = ptr,
					let value = bindAtExec[Int(bitPattern: ptrBang)] else {
					throw ODBCError.error("Could not find bound parameter")
				}
				switch value.value {
				case var value as String:
					try check(value.withUTF8 {
						SQLPutData(hstmt, SQLPOINTER(mutating: $0.baseAddress), $0.count)
					})
				case var value as Data:
					try check(value.withUnsafeMutableBytes {
						SQLPutData(hstmt, $0.baseAddress, $0.count)
					})
				case var value as [UInt8]:
					try check(SQLPutData(hstmt, &value, value.count))
				case var value as [Int8]:
					try check(SQLPutData(hstmt, &value, value.count))
				default:
					throw ODBCError.error("Unusable bound parameter type")
				}
			}
		}
		try check(rc)
	}
}

extension ODBCStatement {
	private func bindNull(number: Int, valueType: ODBCDataType, paramType: ODBCDataType) throws {
		if let b = bindValues {
			try b.advanced(by: number-1).withMemoryRebound(to: Int.self, capacity: 1) {
				p in
				p.initialize(to: SQLLEN(SQL_NULL_DATA))
				try check(SQLBindParameter(hstmt,
											SQLUSMALLINT(number),
											SQLSMALLINT(SQL_PARAM_INPUT),
											valueType.rawValue,
											paramType.rawValue,
											0, 0, nil, 0, p))
			}
		}
	}
	private func bind(number: Int, valueType: ODBCDataType, paramType: ODBCDataType, ptr: SQLPOINTER) throws {
		try check(SQLBindParameter(hstmt,
								   SQLUSMALLINT(number),
								   SQLSMALLINT(SQL_PARAM_INPUT),
								   valueType.rawValue,
								   paramType.rawValue,
								   0, 0, ptr, 0, nil))
	}
	
	public func bindParameter(number: Int, value: String?) throws {
		if let value = value, let b = bindValues {
			let b = b.advanced(by: number-1)
			b.withMemoryRebound(to: Int.self, capacity: 1) {
				p in
				p.initialize(to: Int(SQL_DATA_AT_EXEC)) // should check if driver requires length
														// if so, perform the below instead
//				value.withUTF8 {
//					u in
//					p.initialize(to: Int(_SQL_LEN_DATA_AT_EXEC(Int32(u.count))))
//				}
			}
			bindAtExec[number] = .init(value: value, type: .varchar)
			try check(SQLBindParameter(hstmt,
										SQLUSMALLINT(number),
										SQLSMALLINT(SQL_PARAM_INPUT),
										ODBCDataType.cdefault.rawValue, // pgsql driver, at least, requires this
										ODBCDataType.varchar.rawValue,
										0, 0,
										SQLPOINTER(bitPattern: number), 0, b))
		} else {
			try bindNull(number: number, valueType: .cdefault, paramType: .varchar)
		}
	}
	public func bindParameter(number: Int, value: Data?) throws {
		if let value = value, let b = bindValues {
			let b = b.advanced(by: number-1)
			b.withMemoryRebound(to: Int.self, capacity: 1) {
				p in
				p.initialize(to: Int(SQL_DATA_AT_EXEC))
			}
			bindAtExec[number] = .init(value: value, type: .varbinary)
			try check(SQLBindParameter(hstmt,
										SQLUSMALLINT(number),
										SQLSMALLINT(SQL_PARAM_INPUT),
										ODBCDataType.cdefault.rawValue,
										ODBCDataType.varbinary.rawValue,
										0, 0,
										SQLPOINTER(bitPattern: number), 0, b))
		} else {
			try bindNull(number: number, valueType: .cdefault, paramType: .varbinary)
		}
	}
	public func bindParameter(number: Int, value: [UInt8]?) throws {
		if let value = value, let b = bindValues {
			let b = b.advanced(by: number-1)
			b.withMemoryRebound(to: Int.self, capacity: 1) {
				p in
				p.initialize(to: Int(SQL_DATA_AT_EXEC))
			}
			bindAtExec[number] = .init(value: value, type: .varbinary)
			try check(SQLBindParameter(hstmt,
										SQLUSMALLINT(number),
										SQLSMALLINT(SQL_PARAM_INPUT),
										ODBCDataType.cdefault.rawValue,
										ODBCDataType.varbinary.rawValue,
										0, 0,
										SQLPOINTER(bitPattern: number), 0, b))
		} else {
			try bindNull(number: number, valueType: .cdefault, paramType: .varbinary)
		}
	}
	public func bindParameter(number: Int, value: [Int8]?) throws {
		if let value = value, let b = bindValues {
			let b = b.advanced(by: number-1)
			b.withMemoryRebound(to: Int.self, capacity: 1) {
				p in
				p.initialize(to: Int(SQL_DATA_AT_EXEC))
			}
			bindAtExec[number] = .init(value: value, type: .varbinary)
			try check(SQLBindParameter(hstmt,
										SQLUSMALLINT(number),
										SQLSMALLINT(SQL_PARAM_INPUT),
										ODBCDataType.cdefault.rawValue,
										ODBCDataType.varbinary.rawValue,
										0, 0,
										SQLPOINTER(bitPattern: number), 0, b))
		} else {
			try bindNull(number: number, valueType: .cdefault, paramType: .varbinary)
		}
	}
	
	public func bindParameter(number: Int, value: Int64?) throws {
		if let value = value, let b = bindValues {
			try b.advanced(by: number-1).withMemoryRebound(to: type(of: value), capacity: 1) {
				p in
				p.initialize(to: value)
				try bind(number: number, valueType: .csbigint, paramType: .bigint, ptr: p)
			}
		} else {
			try bindNull(number: number, valueType: .csbigint, paramType: .bigint)
		}
	}
	public func bindParameter(number: Int, value: UInt64?) throws {
		if let value = value, let b = bindValues {
			try b.advanced(by: number-1).withMemoryRebound(to: type(of: value), capacity: 1) {
				p in
				p.initialize(to: value)
				try bind(number: number, valueType: .cubigint, paramType: .bigint, ptr: p)
			}
		} else {
			try bindNull(number: number, valueType: .cubigint, paramType: .bigint)
		}
	}
	public func bindParameter(number: Int, value: Int?) throws {
		if let v = value {
			try bindParameter(number: number, value: Int64(v))
		} else {
			try bindNull(number: number, valueType: .csbigint, paramType: .bigint)
		}
	}
	public func bindParameter(number: Int, value: UInt?) throws {
		if let v = value {
			try bindParameter(number: number, value: UInt64(v))
		} else {
			try bindNull(number: number, valueType: .cubigint, paramType: .bigint)
		}
	}
	public func bindParameter(number: Int, value: Int32?) throws {
		if let value = value, let b = bindValues {
			try b.advanced(by: number-1).withMemoryRebound(to: type(of: value), capacity: 1) {
				p in
				p.initialize(to: value)
				try bind(number: number, valueType: .cslong, paramType: .integer, ptr: p)
			}
		} else {
			try bindNull(number: number, valueType: .cslong, paramType: .integer)
		}
	}
	public func bindParameter(number: Int, value: UInt32?) throws {
		if let value = value, let b = bindValues {
			try b.advanced(by: number-1).withMemoryRebound(to: type(of: value), capacity: 1) {
				p in
				p.initialize(to: value)
				try bind(number: number, valueType: .culong, paramType: .integer, ptr: p)
			}
		} else {
			try bindNull(number: number, valueType: .culong, paramType: .integer)
		}
	}
	public func bindParameter(number: Int, value: Int16?) throws {
		if let value = value, let b = bindValues {
			try b.advanced(by: number-1).withMemoryRebound(to: type(of: value), capacity: 1) {
				p in
				p.initialize(to: value)
				try bind(number: number, valueType: .csshort, paramType: .integer, ptr: p)
			}
		} else {
			try bindNull(number: number, valueType: .csshort, paramType: .integer)
		}
	}
	public func bindParameter(number: Int, value: UInt16?) throws {
		if let value = value, let b = bindValues {
			try b.advanced(by: number-1).withMemoryRebound(to: type(of: value), capacity: 1) {
				p in
				p.initialize(to: value)
				try bind(number: number, valueType: .cushort, paramType: .integer, ptr: p)
			}
		} else {
			try bindNull(number: number, valueType: .cushort, paramType: .integer)
		}
	}
	public func bindParameter(number: Int, value: Int8?) throws {
		if let value = value, let b = bindValues {
			try b.advanced(by: number-1).withMemoryRebound(to: type(of: value), capacity: 1) {
				p in
				p.initialize(to: value)
				try bind(number: number, valueType: .cstinyint, paramType: .tinyint, ptr: p)
			}
		} else {
			try bindNull(number: number, valueType: .cstinyint, paramType: .tinyint)
		}
	}
	public func bindParameter(number: Int, value: UInt8?) throws {
		if let value = value, let b = bindValues {
			try b.advanced(by: number-1).withMemoryRebound(to: type(of: value), capacity: 1) {
				p in
				p.initialize(to: value)
				try bind(number: number, valueType: .cutinyint, paramType: .tinyint, ptr: p)
			}
		} else {
			try bindNull(number: number, valueType: .cutinyint, paramType: .tinyint)
		}
	}
	public func bindParameter(number: Int, value: Bool?) throws {
		if let value = value, let b = bindValues {
			try b.advanced(by: number-1).withMemoryRebound(to: UInt8.self, capacity: 1) {
				p in
				p.initialize(to: value ? 1 : 0)
				try bind(number: number, valueType: .cutinyint, paramType: .bit, ptr: p)
			}
		} else {
			try bindNull(number: number, valueType: .cutinyint, paramType: .bit)
		}
	}
	
	public func bindParameter(number: Int, value: Double?) throws {
		if let value = value, let b = bindValues {
			try b.advanced(by: number-1).withMemoryRebound(to: type(of: value), capacity: 1) {
				p in
				p.initialize(to: value)
				try bind(number: number, valueType: .double, paramType: .double, ptr: p)
			}
		} else {
			try bindNull(number: number, valueType: .double, paramType: .double)
		}
	}
	public func bindParameter(number: Int, value: Float?) throws {
		if let value = value, let b = bindValues {
			try b.advanced(by: number-1).withMemoryRebound(to: type(of: value), capacity: 1) {
				p in
				p.initialize(to: value)
				try bind(number: number, valueType: .real, paramType: .real, ptr: p)
			}
		} else {
			try bindNull(number: number, valueType: .real, paramType: .real)
		}
	}
	public func bindParameter(number: Int, value: UUID?) throws {
		if let value = value {//}, let b = bindValues {
			try bindParameter(number: number, value: value.uuidString)
			// using SQLGUID is not supported by MSSQL
			// work in Postgres tho, but so does string conversion
//			let sqlGUID = SQLGUID(value.uuid)
//			var data = Data(count: MemoryLayout.size(ofValue: sqlGUID))
//			try check(data.withUnsafeMutableBytes {
//				uuPtr in
//				uuPtr.bindMemory(to: SQLGUID.self).initialize(repeating: sqlGUID)
//				return b.advanced(by: number-1).withMemoryRebound(to: SQLLEN.self, capacity: 1) {
//					lenPtr in
//					lenPtr.initialize(to: uuPtr.count)
//					return SQLBindParameter(hstmt,
//											SQLUSMALLINT(number),
//											SQLSMALLINT(SQL_PARAM_INPUT),
//											ODBCDataType.cdefault.rawValue,
//											ODBCDataType.guid.rawValue,
//											0, 0,
//											uuPtr.baseAddress, 0, lenPtr)
//				}
//			})
//			// this is not actually bound at exec but we keep the Data alive here
//			bindAtExec[number] = .init(value: data, type: ODBCDataType.guid)
		} else {
			try bindNull(number: number, valueType: .cdefault, paramType: .guid)
		}
	}
}

extension ODBCStatement {
	public func getData(number: Int, buffer: UnsafeMutableRawBufferPointer, type: ODBCDataType = .cdefault) throws -> Int? {
		var len: SQLLEN = 0
		try check(SQLGetData(hstmt, SQLUSMALLINT(number), type.rawValue, buffer.baseAddress, buffer.count, &len))
		guard len != SQL_NULL_DATA else {
			return nil
		}
		return Int(len)
	}
	public func getData(number: Int, buffer: Data, type: ODBCDataType = .cdefault) throws -> Int? {
		var buffer = buffer
		return try buffer.withUnsafeMutableBytes {
			try getData(number: number, buffer: $0, type: type)
		}
	}
	public func getData(number: Int, estimatedSize: Int = 256) throws -> Data? {
		var firstBuff = Data(count: estimatedSize)
		guard let fullLen = try getData(number: number, buffer: firstBuff, type: .binary) else {
			return nil
		}
		if fullLen > estimatedSize {
			var nextBuf = Data(count: fullLen)
			nextBuf.append(firstBuff)
			firstBuff = nextBuf
			var len: SQLLEN = 0
			try check(firstBuff.withUnsafeMutableBytes {
				SQLGetData(hstmt, SQLUSMALLINT(number), ODBCDataType.binary.rawValue, $0.baseAddress!.advanced(by: estimatedSize-1), fullLen, &len)
			})
		}
		if fullLen < firstBuff.count {
			firstBuff.removeSubrange(Range(uncheckedBounds: (lower: fullLen, upper: firstBuff.count)))
		}
		assert(firstBuff.count == fullLen)
		return firstBuff
	}
	public func getData(number: Int, estimatedSize: Int = 256, encoding: String.Encoding = .utf8) throws -> String? {
		var firstBuff = UnsafeMutableRawBufferPointer.allocate(byteCount: estimatedSize, alignment: 0)
		defer {
			firstBuff.deallocate()
		}
		guard let fullLen = try getData(number: number, buffer: firstBuff, type: .char),
			fullLen >= 0 else {
			return nil
		}
		if fullLen == 0 {
			return ""
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
	
	public func getData(number: Int) throws -> UUID? {
		if let s: String = try getData(number: number) {
			return UUID(uuidString: s)
		}
		return nil
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
		try check(SQLGetData(hstmt, SQLUSMALLINT(number), ODBCDataType.real.rawValue, &val, MemoryLayout.size(ofValue: val), &len))
		guard len != SQL_NULL_DATA else {
			return nil
		}
		return val
	}
	
	public func getData(number: Int) throws -> Bool? {
		var len: SQLLEN = 0
		var val: Int8 = 0
		try check(SQLGetData(hstmt, SQLUSMALLINT(number), ODBCDataType.cstinyint.rawValue, &val, MemoryLayout.size(ofValue: val), &len))
		guard len != SQL_NULL_DATA else {
			return nil
		}
		return val == 1
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
}
