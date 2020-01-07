//
//  File.swift
//  
//
//  Created by Kyle Jessup on 2020-01-03.
//

import Foundation
import unixodbc

public class ODBCConnection: ODBCHandle {
	var hdbc: SQLHDBC? { super.handle as SQLHDBC? }
	var henv: SQLHENV? { environment.henv }
	public let environment: ODBCEnvironment
	deinit {
		SQLDisconnect(hdbc)
	}
	init(env: ODBCEnvironment, dsn: String, user: String, pass: String) throws {
		self.environment = env
		super.init(type: SQL_HANDLE_DBC)
		try check(SQLAllocHandle(type, henv, &handle))
		var dsn = dsn, user = user, pass = pass
		try check(dsn.withUTF8 { dbs in
			return user.withUTF8 { us in
				return pass.withUTF8 { ps in
					return SQLConnect(hdbc,
							  UnsafeMutablePointer<UInt8>(mutating: dbs.baseAddress), SQLSMALLINT(dbs.count),
							  UnsafeMutablePointer<UInt8>(mutating: us.baseAddress), SQLSMALLINT(us.count),
							  UnsafeMutablePointer<UInt8>(mutating: ps.baseAddress), SQLSMALLINT(ps.count))
				}
			}
		})
	}
	public var driverVersion: String? {
		return getInfo(SQL_DRIVER_VER)
	}
	public var serverVersion: String? {
		return getInfo(SQL_DBMS_VER)
	}
	public var serverName: String? {
		return getInfo(SQL_DBMS_NAME)
	}
	public var isAlive: Bool {
		var val: SQLUINTEGER = 0
		return SQL_SUCCEEDED(SQLGetConnectAttr(hdbc, SQL_ATTR_CONNECTION_DEAD, &val, SQL_IS_UINTEGER, nil)) && SQL_CD_TRUE != val
	}
	public func tables() throws -> [String] {
		var ret: [String] = []
		var stat = try ODBCStatement(con: self)
		if SQL_SUCCEEDED(SQLTables(stat.hstmt, nil, 0, nil, 0, nil, 0, nil, 0)) {
			let name = UnsafeMutableBufferPointer<SQLCHAR>.allocate(capacity: bigNameLen)
			defer {
				name.deallocate()
			}
			var nameLen: SQLLEN = 0
			SQLBindCol(stat.hstmt, 3, SQLSMALLINT(SQL_C_CHAR), name.baseAddress, name.count, &nameLen)
			while SQL_SUCCEEDED(SQLFetch(stat.hstmt))	{
				if let s = String(bytes: name[0..<Int(nameLen)], encoding: .utf8) {
					ret.append(s)
				}
			}
		}
		return ret
	}
	private func getInfo(_ type: Int32) -> String? {
		let s = UnsafeMutableBufferPointer<SQLCHAR>.allocate(capacity: bigNameLen)
		defer {
			s.deallocate()
		}
		var len: SQLSMALLINT = 0
		SQLGetInfo(hdbc, SQLUSMALLINT(type), s.baseAddress, SQLSMALLINT(s.count), &len)
		guard let str = String(bytes: s[0..<Int(len)], encoding: .utf8) else {
			return nil
		}
		return str
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
			SQLError(henv, hdbc, nil,
					 sqlState.baseAddress,
					 &errCode,
					 errMsg.baseAddress,
					 SQLSMALLINT(maxMsgSize),
					 &errorSize)
			let msgStr = "[\(String(bytes: sqlState[0..<5], encoding: .utf8) ?? "")]\(String(bytes: errMsg[0..<Int(errorSize)], encoding: .utf8) ?? "")"
			throw ODBCError.error(msgStr)
		}
	}
	public func commit() throws {
		try check(SQLEndTran(type, hdbc, SQLSMALLINT(SQL_COMMIT)))
	}
	public func rollback() throws {
		try check(SQLEndTran(type, hdbc, SQLSMALLINT(SQL_ROLLBACK)))
	}
	public func prepare(statement: String) throws -> ODBCStatement {
		let stat = try ODBCStatement(con: self)
		var statement = statement
		try check(statement.withUTF8 {
			s in
			return SQLPrepare(stat.hstmt,
							  UnsafeMutablePointer<UInt8>(mutating: s.baseAddress),
							  SQLINTEGER(s.count))
		})
		return stat
	}
	@discardableResult
	public func execute(statement: String) throws -> ODBCStatement {
		let stat = try ODBCStatement(con: self)
		var statement = statement
		try stat.check(statement.withUTF8 {
			s in
			return SQLExecDirect(stat.hstmt,
							  UnsafeMutablePointer<UInt8>(mutating: s.baseAddress),
							  SQLINTEGER(s.count))
		})
		return stat
	}
}
