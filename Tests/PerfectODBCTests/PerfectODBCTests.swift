import XCTest
@testable import PerfectODBC

// these tests assume a PostgreSQL ODBC driver is installed and can be accessed with un postgres and no pw

class PerfectODBCTests: XCTestCase {
    func testConnection() throws {
        let c = try ODBCConnection(dsn: "PostgreSQL", user: "postgres", pass: "")
		XCTAssertTrue(c.isAlive)
		let a = try c.tables()
		print("\(a)")
		print("\(c.driverVersion)")
		print("\(c.serverVersion)")
		print("\(c.serverName)")
    }

	func testDataSources() {
		let a = ODBCConnection.datasources()
		print("\(a)")
	}
}
