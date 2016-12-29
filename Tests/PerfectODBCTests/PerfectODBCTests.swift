import XCTest
@testable import PerfectODBC

class PerfectODBCTests: XCTestCase {
    func testExample() {
        // This is an example of a functional test case.
        // Use XCTAssert and related functions to verify your tests produce the correct results.
        XCTAssertEqual(PerfectODBC().text, "Hello, World!")
    }


    static var allTests : [(String, (PerfectODBCTests) -> () throws -> Void)] {
        return [
            ("testExample", testExample),
        ]
    }
}
