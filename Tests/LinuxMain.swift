import XCTest
@testable import PostgresODBCTests
@testable import MSSQLODBCTests

XCTMain([
	testCase(PostgresODBCTests.allTests),
	testCase(MSSQLODBCTests.allTests),
])
