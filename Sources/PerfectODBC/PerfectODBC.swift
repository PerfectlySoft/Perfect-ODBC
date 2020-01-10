
import Foundation
import unixodbc

// oracle seems to have the longest possible names at 128 bytes
// pad a little bit for the unknown
let bigNameLen = 256

func SQL_SUCCEEDED(_ code: SQLRETURN) -> Bool {
	return (((code) & (~1)) == 0)
}

public enum ODBCError : Error, CustomStringConvertible {
	public var description: String {
		switch self {
		case .error(let s):
			return s
		}
	}
	/// Error with detail message.
	case error(String)
}

public class ODBCHandle {
	var handle: SQLHANDLE?
	let handleType: SQLSMALLINT
	deinit {
		SQLFreeHandle(handleType, handle)
	}
	init(type: Int32) {
		self.handleType = SQLSMALLINT(type)
	}
}
