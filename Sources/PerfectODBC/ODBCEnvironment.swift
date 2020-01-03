//
//  File.swift
//  
//
//  Created by Kyle Jessup on 2020-01-03.
//

import Foundation
import unixodbc

public class ODBCEnvironment: ODBCHandle {
	static let gInit: Bool = {
		SQLSetEnvAttr(nil, _SQL_ATTR_CONNECTION_POOLING, _SQL_CP_ONE_PER_DRIVER, SQL_IS_INTEGER)
		return true
	}()
	var henv: SQLHENV? { super.handle as SQLHENV? }
	public init() {
		_ = ODBCEnvironment.gInit
		super.init(type: SQL_HANDLE_ENV)
		SQLAllocHandle(type, nil, &handle)
		SQLSetEnvAttr(henv, _SQL_ATTR_ODBC_VERSION, _SQL_OV_ODBC3, 0)
	}
	public func datasources() -> [String] {
		let currDsName = UnsafeMutableBufferPointer<SQLCHAR>.allocate(capacity: bigNameLen)
		let desc = UnsafeMutableBufferPointer<SQLCHAR>.allocate(capacity: bigNameLen)
		defer {
			currDsName.deallocate()
			desc.deallocate()
		}
		var realLen: SQLSMALLINT = 0
		var descLen: SQLSMALLINT = 0
		var ret: [String] = []
		if SQL_SUCCEEDED(SQLDataSources(henv, SQLUSMALLINT(SQL_FETCH_FIRST),
										currDsName.baseAddress, SQLSMALLINT(currDsName.count), &realLen,
										desc.baseAddress, SQLSMALLINT(desc.count), &descLen)) {
			if let s = String(bytes: currDsName[0..<Int(realLen)], encoding: .utf8) {
				ret.append(s)
			}
			while SQL_SUCCEEDED(SQLDataSources(henv, SQLUSMALLINT(SQL_FETCH_NEXT),
											   currDsName.baseAddress, SQLSMALLINT(currDsName.count), &realLen,
											   desc.baseAddress, SQLSMALLINT(desc.count), &descLen)) {
												if let s = String(bytes: currDsName[0..<Int(realLen)], encoding: .utf8) {
													ret.append(s)
												}
			}
		}
		return ret
	}
	public func connect(dsn: String, user: String, pass: String) throws -> ODBCConnection {
		return try ODBCConnection(env: self, dsn: dsn, user: user, pass: pass)
	}
}
