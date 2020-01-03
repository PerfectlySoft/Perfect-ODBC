
import Foundation
import unixodbc

// oracle seems to have the longest possible names at 128 bytes
// pad a little bit for the unknown
let bigNameLen = 256

func SQL_SUCCEEDED(_ code: SQLRETURN) -> Bool {
	return (((code) & (~1)) == 0)
}

public enum ODBCError : Error {
	/// Error with detail message.
	case error(String)
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
