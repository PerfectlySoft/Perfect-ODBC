
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

public class ODBCConnection {
	static let gInit: Bool = {
		SQLSetEnvAttr(nil, _SQL_ATTR_CONNECTION_POOLING, _SQL_CP_ONE_PER_DRIVER, SQL_IS_INTEGER)
		return true
	}()
	
	var henv: SQLHENV?
	var hdbc: SQLHDBC?
	
	static func datasources() -> [String] {
		let currDsName = UnsafeMutableBufferPointer<SQLCHAR>.allocate(capacity: 1024)
		let desc = UnsafeMutableBufferPointer<SQLCHAR>.allocate(capacity: 1024)
		var realLen: SQLSMALLINT = 0
		var descLen: SQLSMALLINT = 0
		var ret: [String] = []
		var henv: SQLHENV?
		
		SQLAllocHandle(SQLSMALLINT(SQL_HANDLE_ENV), nil, &henv)
		SQLSetEnvAttr(henv, _SQL_ATTR_ODBC_VERSION, _SQL_OV_ODBC3, 0)
		defer {
			SQLFreeEnv(henv)
		}
		
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
	
	deinit {
		if let hdbc = self.hdbc {
			SQLDisconnect(hdbc)
			SQLFreeConnect(hdbc)
		}
		if let henv = self.henv {
			SQLFreeEnv(henv)
		}
	}
	
	init(dsn: String, user: String, pass: String) throws {
		_ = ODBCConnection.gInit
		try check(SQLAllocHandle(SQLSMALLINT(SQL_HANDLE_ENV), nil, &henv))
		SQLSetEnvAttr(henv, _SQL_ATTR_ODBC_VERSION, _SQL_OV_ODBC3, 0)
		try check(SQLAllocHandle(SQLSMALLINT(SQL_HANDLE_DBC), henv, &hdbc))
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
	
	func check(_ code: SQLRETURN) throws {
		guard SQL_SUCCEEDED(code) else {
			let maxMsgSize = 256
			let sqlState = UnsafeMutableBufferPointer<SQLCHAR>.allocate(capacity: 6)
			let errMsg = UnsafeMutableBufferPointer<SQLCHAR>.allocate(capacity: maxMsgSize)
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
	
	func tables() throws -> [String] {
		var ret: [String] = []
		var stat: SQLHSTMT?
		guard SQL_SUCCEEDED(SQLAllocHandle(SQLSMALLINT(SQL_HANDLE_STMT), hdbc, &stat)) else {
			return ret
		}
		defer {
			SQLFreeHandle(SQLSMALLINT(SQL_HANDLE_STMT), stat)
		}
		if SQL_SUCCEEDED(SQLTables(stat, nil, 0, nil, 0, nil, 0, nil, 0)) {
			let name = UnsafeMutableBufferPointer<SQLCHAR>.allocate(capacity: 1024)
			var nameLen: SQLLEN = 0
			SQLBindCol(stat, 3, SQLSMALLINT(SQL_C_CHAR), name.baseAddress, name.count, &nameLen)
			
			while SQL_SUCCEEDED(SQLFetch(stat))	{
				if let s = String(bytes: name[0..<Int(nameLen)], encoding: .utf8) {
					ret.append(s)
				}
			}
		}
		return ret
	}
}

public class ODBCCursor {
	
}
