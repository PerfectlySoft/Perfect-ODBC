import XCTest
@testable import PerfectODBC

class PerfectODBCTests: XCTestCase {
    func testConnection() throws {
        let c = try ODBCConnection(dsn: "PostgreSQL", user: "postgres", pass: "")
		let a = try c.tables()
		print("\(a)")
    }

	func testDataSources() {
		let a = ODBCConnection.datasources()
		print("\(a)")
	}
}
