import XCTest
@testable import PerfectODBC

// these tests assume a SQL Server ODBC driver is installed and can be accessed with
// Database master, UID SA, and PWD <YourStrong@Passw0rd>
let t1create =
"""
CREATE TABLE "mssql_test" (
	id bigint NOT NULL,
	"byte" tinyint,
	"smallint" smallint,
	"integer" int,
	"bigint" bigint,
	"real" real,
	"double" float,
	"text" varchar(4000) COLLATE Latin1_General_100_CI_AS_SC_UTF8,
	"bytea" varbinary(4096),
	"boolean" bit,
	"uuid" uniqueidentifier,
	CONSTRAINT mssql_test_pkey PRIMARY KEY (id)
);
"""
let UID = "SA"
let PWD = "<YourStrong@Passw0rd>"
let DSN = "master"
let numColumns = 11

class MSSQLODBCTests: XCTestCase {
	override func setUp() {
		let env = ODBCEnvironment()
		let c = try! env.connect(dsn: DSN, user: UID, pass: PWD)
		try! c.execute(statement: "DROP TABLE IF EXISTS \"mssql_test\"")
		try! c.execute(statement: t1create)
	}
	
	override func tearDown() {
//		let env = ODBCEnvironment()
//		let c = try! env.connect(dsn: DSN, user: UID, pass: PWD)
//		try! c.execute(statement: "DROP TABLE IF EXISTS \"mssql_test\"")
	}
	
	func testConnectionMSSQL() throws {
		let env = ODBCEnvironment()
		let c = try env.connect(dsn: DSN, user: UID, pass: PWD)
		XCTAssertTrue(c.isAlive)
		XCTAssert(c.driverVersion!.hasPrefix("17."))
		XCTAssert(c.serverVersion!.hasPrefix("15."))
		XCTAssertEqual(c.serverName, "Microsoft SQL Server")
	}
	
	func testDataSources() {
		let a = ODBCEnvironment().datasources()
		XCTAssert(a.contains(DSN))
	}
	
	func testTables() throws {
		let env = ODBCEnvironment()
		let c = try env.connect(dsn: DSN, user: UID, pass: PWD)
		let tables = try c.tables()
		XCTAssert(tables.contains("mssql_test"))
	}
	
	func testParams() throws {
		let testString = "🛹" +  String(repeating: "1", count: 1024)
		let testData = Data(repeating: 0, count: 2048)
		let testUUID = UUID()
		
		let env = ODBCEnvironment()
		let c = try env.connect(dsn: DSN, user: UID, pass: PWD)
		
//		do {
//			let s = try c.prepare(statement:
//				"""
//				INSERT INTO "mssql_test"
//					(id, "uuid")
//					VALUES
//					(?,?)
//				""")
//			// three rows: mins, maxs, nulls
//			do {
//				try s.bindParameter(number: 1, value: 0)
//				try s.bindParameter(number: 2, value: testUUID)
//			}
//			try s.execute()
//		}
		
		do {
			let s = try c.prepare(statement:
				"""
				INSERT INTO "mssql_test"
					(id, "byte", "smallint", "integer", "bigint", "real",
						"double", "text", "bytea", "boolean", "uuid")
					VALUES
					(?,?,?,?,?,?,?,?,?,?,?)
				""")
			XCTAssertEqual(try s.numParams(), numColumns)
			// three rows: mins, maxs, nulls
			do {
				try s.bindParameter(number: 1, value: 1)
				try s.bindParameter(number: 2, value: UInt8.min)
				try s.bindParameter(number: 3, value: Int16.min)
				try s.bindParameter(number: 4, value: Int32.min)
				try s.bindParameter(number: 5, value: Int64.min)
				try s.bindParameter(number: 6, value: Float.leastNormalMagnitude)
				try s.bindParameter(number: 7, value: Double.leastNormalMagnitude)
				try s.bindParameter(number: 8, value: testString)
				try s.bindParameter(number: 9, value: testData)
				try s.bindParameter(number: 10, value: true)
				try s.bindParameter(number: 11, value: testUUID)
			}
			try s.execute()
			try s.closeCursor()
			do {
				try s.bindParameter(number: 1, value: 2)
				try s.bindParameter(number: 2, value: UInt8.max)
				try s.bindParameter(number: 3, value: Int16.max)
				try s.bindParameter(number: 4, value: Int32.max)
				try s.bindParameter(number: 5, value: Int64.max)
				try s.bindParameter(number: 6, value: Float.greatestFiniteMagnitude)
				try s.bindParameter(number: 7, value: Double.greatestFiniteMagnitude)
				try s.bindParameter(number: 8, value: testString)
				try s.bindParameter(number: 9, value: testData)
				try s.bindParameter(number: 10, value: false)
				try s.bindParameter(number: 11, value: testUUID)
			}
			try s.execute()
			try s.closeCursor()
			do {
				try s.bindParameter(number: 1, value: 3)
				try s.bindParameter(number: 2, value: nil as UInt8?)
				try s.bindParameter(number: 3, value: nil as Int16?)
				try s.bindParameter(number: 4, value: nil as Int32?)
				try s.bindParameter(number: 5, value: nil as Int64?)
				try s.bindParameter(number: 6, value: nil as Float?)
				try s.bindParameter(number: 7, value: nil as Double?)
				try s.bindParameter(number: 8, value: nil as String?)
				try s.bindParameter(number: 9, value: nil as Data?)
				try s.bindParameter(number: 10, value: nil as Bool?)
				try s.bindParameter(number: 11, value: nil as UUID?)
			}
			try s.execute()
		}
		do {
			let s = try c.prepare(statement:
				"""
				SELECT * FROM "mssql_test" WHERE id = ?
				""")
			do {
				try s.bindParameter(number: 1, value: 1)
				try s.execute()
				let fetchRes = try s.fetch()
				XCTAssertEqual(fetchRes, .success)
				XCTAssertEqual(UInt8.min, try s.getData(number: 2))
				XCTAssertEqual(Int16.min, try s.getData(number: 3))
				XCTAssertEqual(Int32.min, try s.getData(number: 4))
				XCTAssertEqual(Int64.min, try s.getData(number: 5))
				XCTAssertEqual(Float.leastNormalMagnitude, try s.getData(number: 6))
				XCTAssertEqual(Double.leastNormalMagnitude, try s.getData(number: 7))
				XCTAssertEqual(testString, try s.getData(number: 8))
				XCTAssertEqual(testData, try s.getData(number: 9))
				XCTAssertEqual(true, try s.getData(number: 10))
				XCTAssertEqual(testUUID, try s.getData(number: 11))
			}
			do {
				try s.closeCursor()
				try s.bindParameter(number: 1, value: 2)
				try s.execute()
				let fetchRes = try s.fetch()
				XCTAssertEqual(fetchRes, .success)
				XCTAssertEqual(UInt8.max, try s.getData(number: 2))
				XCTAssertEqual(Int16.max, try s.getData(number: 3))
				XCTAssertEqual(Int32.max, try s.getData(number: 4))
				XCTAssertEqual(Int64.max, try s.getData(number: 5))
				XCTAssertEqual(Float.greatestFiniteMagnitude, try s.getData(number: 6))
				XCTAssertEqual(Double.greatestFiniteMagnitude, try s.getData(number: 7))
				XCTAssertEqual(testString, try s.getData(number: 8))
				XCTAssertEqual(testData, try s.getData(number: 9))
				XCTAssertEqual(false, try s.getData(number: 10))
				XCTAssertEqual(testUUID, try s.getData(number: 11))
			}
			do {
				try s.closeCursor()
				try s.bindParameter(number: 1, value: 3)
				try s.execute()
				let fetchRes = try s.fetch()
				XCTAssertEqual(fetchRes, .success)
				XCTAssertEqual(nil as UInt8?, try s.getData(number: 2))
				XCTAssertEqual(nil as Int16?, try s.getData(number: 3))
				XCTAssertEqual(nil as Int32?, try s.getData(number: 4))
				XCTAssertEqual(nil as Int64?, try s.getData(number: 5))
				XCTAssertEqual(nil as Float?, try s.getData(number: 6))
				XCTAssertEqual(nil as Double?, try s.getData(number: 7))
				XCTAssertEqual(nil as String?, try s.getData(number: 8))
				XCTAssertEqual(nil as Data?, try s.getData(number: 9))
				XCTAssertEqual(nil as Bool?, try s.getData(number: 10))
				XCTAssertEqual(nil as UUID?, try s.getData(number: 11))
			}
		}
	}
}
