
import Foundation
import unixodbc

public enum ODBCError : Error {
	/// Error with detail message.
	case error(String)
}

struct ODBCCommandHandles {
	let hstmt: SQLHSTMT
}

func SQL_SUCCEEDED(_ code: SQLRETURN) -> Bool {
	return (((code) & (~1)) == 0)
}

public class ODBCHandle {
	var handle: SQLHANDLE?
	let type: SQLSMALLINT
	deinit {
		SQLFreeHandle(type, handle)
	}
	init(type: Int32) {
		self.type = SQLSMALLINT(type)
	}
}

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
		let currDsName = UnsafeMutableBufferPointer<SQLCHAR>.allocate(capacity: 1024)
		let desc = UnsafeMutableBufferPointer<SQLCHAR>.allocate(capacity: 1024)
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
			let name = UnsafeMutableBufferPointer<SQLCHAR>.allocate(capacity: 1024)
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
		let s = UnsafeMutableBufferPointer<SQLCHAR>.allocate(capacity: 1024)
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
			let maxMsgSize = 256
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
	public func execute(statement: String) throws -> ODBCStatement {
		let stat = try ODBCStatement(con: self)
		var statement = statement
		try check(statement.withUTF8 {
			s in
			return SQLExecDirect(stat.hstmt,
							  UnsafeMutablePointer<UInt8>(mutating: s.baseAddress),
							  SQLINTEGER(s.count))
		})
		return stat
	}
}

public class ODBCStatement: ODBCHandle {
	public enum FetchResult {
		case success, noData, stillExecuting
	}
	public enum MoreResult {
		case success, noData, stillExecuting, paramDataAvailable
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
	public func numResultCols() throws -> Int {
		var num: SQLSMALLINT = 0
		try connection.check(SQLNumResultCols(hstmt, &num))
		return Int(num)
	}
}
