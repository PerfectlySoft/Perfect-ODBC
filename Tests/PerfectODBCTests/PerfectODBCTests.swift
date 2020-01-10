import XCTest
@testable import PerfectODBC

// these tests assume a PostgreSQL ODBC driver is installed and can be accessed with un postgres and no pw
let t1create =
"""
CREATE TABLE "TestTable1" (
  id bigint NOT NULL,
  name text,
  "int" bigint,
  doub double precision,
  blob bytea,
  CONSTRAINT TestTable1_pkey PRIMARY KEY (id)
) WITH (OIDS=FALSE);
"""
let t2create =
"""
CREATE TABLE "TestTable2" (
  id bigint NOT NULL,
  parentid bigint,
  name text,
  "int" bigint,
  doub double precision,
  blob bytea,
  CONSTRAINT TestTable2_pkey PRIMARY KEY (id)
) WITH (OIDS=FALSE);
"""

class PerfectODBCTests: XCTestCase {
	override func setUp() {
//		let env = ODBCEnvironment()
//		let c = try! env.connect(dsn: "PostgreSQL", user: "postgres", pass: "")
//		try! c.execute(statement: "DROP TABLE IF EXISTS \"TestTable1\"")
//		try! c.execute(statement: "DROP TABLE IF EXISTS \"TestTable2\"")
//		try! c.execute(statement: t1create)
//		try! c.execute(statement: t2create)
	}
	
	override func tearDown() {
//		let env = ODBCEnvironment()
//		let c = try! env.connect(dsn: "PostgreSQL", user: "postgres", pass: "")
//		try! c.execute(statement: "DROP TABLE IF EXISTS \"TestTable1\"")
//		try! c.execute(statement: "DROP TABLE IF EXISTS \"TestTable2\"")
	}
	
	func testConnection() throws {
		let env = ODBCEnvironment()
		let c = try env.connect(dsn: "PostgreSQL", user: "postgres", pass: "")
		XCTAssertTrue(c.isAlive)
		XCTAssert(c.driverVersion!.hasPrefix("12."))
		XCTAssert(c.serverVersion!.hasPrefix("9."))
		XCTAssertEqual(c.serverName, "PostgreSQL")
	}
	
	func testDataSources() {
		let a = ODBCEnvironment().datasources()
		XCTAssert(a.contains("PostgreSQL"))
	}
	
	func testTables() throws {
		let env = ODBCEnvironment()
		let c = try env.connect(dsn: "PostgreSQL", user: "postgres", pass: "")
		let tables = try c.tables()
		XCTAssert(tables.contains("TestTable1"))
		XCTAssert(tables.contains("TestTable2"))
	}
	
	func testParams() throws {
		let env = ODBCEnvironment()
		let c = try env.connect(dsn: "PostgreSQL", user: "postgres", pass: "")
		do {
			let s = try c.prepare(statement: "SELECT COUNT(*) FROM \"TestTable1\" WHERE \"int\" = ?")
			XCTAssertEqual(try s.numParams(), 1)
			try s.bindParameter(number: 1, value: 42)
			try s.execute()
			while case .success = try s.fetch() {
				let colDesc = try s.describeColumn(number: 1)
				print("\(colDesc)")
				let id: Int? = try s.getData(number: 1)
				XCTAssertEqual(id, 1)
			}
		}
		do {
			let s = try c.prepare(statement: "SELECT COUNT(*) FROM \"TestTable1\" WHERE name = ?")
			XCTAssertEqual(try s.numParams(), 1)
			try s.bindParameter(number: 1, value: "the name")
			try s.execute()
			while case .success = try s.fetch() {
				let colDesc = try s.describeColumn(number: 1)
				print("\(colDesc)")
				let id: Int? = try s.getData(number: 1)
				XCTAssertEqual(id, 1)
			}
		}
		do {
			let s = try c.prepare(statement: "SELECT COUNT(*) FROM \"TestTable1\" WHERE doub = ?")
			XCTAssertEqual(try s.numParams(), 1)
			try s.bindParameter(number: 1, value: 42.999)
			try s.execute()
			while case .success = try s.fetch() {
				let colDesc = try s.describeColumn(number: 1)
				print("\(colDesc)")
				let id: Int? = try s.getData(number: 1)
				XCTAssertEqual(id, 1)
			}
		}
	}
	
	func testResultColumns() throws {
		let env = ODBCEnvironment()
		let c = try env.connect(dsn: "PostgreSQL", user: "postgres", pass: "")
		let s = try c.prepare(statement: "SELECT id, name, doub FROM \"TestTable1\"")
		try s.execute()
		while case .success = try s.fetch() {
			do {
				let colDesc = try s.describeColumn(number: 1)
				print("\(colDesc)")
				let id: Int? = try s.getData(number: 1)
				print("\(id)")
			}
			do {
				let colDesc = try s.describeColumn(number: 2)
				print("\(colDesc)")
				let s: String? = try s.getData(number: 2)
				print("\(s)")
			}
			do {
				let colDesc = try s.describeColumn(number: 3)
				print("\(colDesc)")
				let doub: Double? = try s.getData(number: 3)
				print("\(doub)")
			}
		}
	}
}
