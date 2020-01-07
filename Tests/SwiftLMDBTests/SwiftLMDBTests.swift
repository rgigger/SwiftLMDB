//
//  SwiftLMDBTests.swift
//  SwiftLMDBTests
//
//  Created by August Heegaard on 29/09/2016.
//  Copyright © 2016 August Heegaard. All rights reserved.
//

import XCTest
import Foundation
@testable import SwiftLMDB

class SwiftLMDBTests: XCTestCase {

    static let envPath: String = {

        // TODO: this should probably also append a random string to the end to make sure that if you
        //       ever have tests running at the same time they won't interfere with each other
        //       it should then delete the directory when everything is done
        let tempURL = URL(fileURLWithPath: NSTemporaryDirectory())
        let envURL = tempURL.appendingPathComponent("SwiftLMDBTests/")
        
        do {
            try FileManager.default.createDirectory(at: envURL, withIntermediateDirectories: true, attributes: nil)
        } catch {
            XCTFail("Could not create DB dir: \(error)")
        }
        
        return envURL.path

    }()
    
    var envPath: String { return SwiftLMDBTests.envPath }
    
    override func setUp() {
        super.setUp()
        // Put setup code here. This method is called before the invocation of each test method in the class.
    }
    
    override func tearDown() {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
        super.tearDown()
        
    }
    
    override class func tearDown() {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
        super.tearDown()

        try? FileManager.default.removeItem(atPath: envPath)
        
    }
    
    // MARK: - Helpers
    
    private func createDatabase(named name: String, flags: Database.Flags = []) -> Database {
        do {
            // this relies on the
            let environment = try Environment(path: envPath, flags: [], maxDBs: 32)
            return try environment.openDatabase(named: #function, flags: [.create])
        } catch {
            XCTFail(error.localizedDescription)
            fatalError()
        }
    }
    
    // Inserts a value and reads it back, verifying that the two values match.
    private func putGetValue<T>(value: T, key: String, in database: Database, withTransaction transaction: Transaction? = nil) where T: DataConvertible & Equatable {
        do {
            try database.put(value: value, forKey: key, withTransaction: transaction)
            let fetchedValue = try database.get(type: type(of: value), forKey: key, withTransaction: transaction)
            XCTAssertEqual(value, fetchedValue, "The returned value does not match the one that was set.")
        } catch {
            XCTFail(error.localizedDescription)
            fatalError()
        }
    }

    private func checkValue<T>(value: T, key: String, in database: Database) where T: DataConvertible & Equatable {
        do {
            let fetchedValue = try database.get(type: type(of: value), forKey: key, withTransaction: nil)
            XCTAssertEqual(value, fetchedValue, "The returned value does not match the expected value.")
        } catch {
            XCTFail(error.localizedDescription)
            fatalError()
        }

    }
    
    // MARK: - Tests
    
    func testGetLMDBVersion() {
        XCTAssert(SwiftLMDB.version != (0, 0, 0), "Unable to get LMDB major version.")
    }
    
    func testCreateEnvironment() {
        
        do {
            _ = try Environment(path: envPath, flags: [], maxDBs: 32, maxReaders: 126, mapSize: 10485760)
        } catch {
            XCTFail(error.localizedDescription)
            return
        }
        
    }
    
    func testCreateUnnamedDatabase() {
        
        do {
            let environment = try Environment(path: envPath, flags: [], maxDBs: 32)
            _ = try environment.openDatabase(named: nil, flags: [.create])
        } catch {
            XCTFail(error.localizedDescription)
            return
        }

    }
    
    func testHasKey() {
        
        let database = createDatabase(named: #function)

        let value = "Hello world!"
        let keyWithValue = "hv1"
        let keyWithoutValue = "hv2"
        
        do {
            try database.put(value: value, forKey: keyWithValue, withTransaction: nil)
            
            let hasValue1 = try database.exists(key: keyWithValue, withTransaction: nil)
            let hasValue2 = try database.exists(key: keyWithoutValue, withTransaction: nil)
            
            XCTAssertEqual(hasValue1, true, "A value has been set for this key. Result should be true.")
            XCTAssertEqual(hasValue2, false, "No value has been set for this key. Result should be false.")
        } catch {
            XCTFail(error.localizedDescription)
        }
        
    }
    
    func testPutGet() {
        
        let database = createDatabase(named: #function)
        
        // Key generating sequence
        var seq = sequence(first: 0, next: { $0 + 1 })
        let nextKey = { "key-\(seq.next()!)" }
        
        // Boolean
        putGetValue(value: true, key: nextKey(), in: database)
        putGetValue(value: false, key: nextKey(), in: database)
        
        // String
        putGetValue(value: "ÆØÅ", key: nextKey(), in: database)
        putGetValue(value: "Hello world! 👋🏼", key: nextKey(), in: database)
        
        // Date
        putGetValue(value: Date(), key: nextKey(), in: database)
        
        // Integers
        putGetValue(value: Int.max, key: nextKey(), in: database)
        putGetValue(value: Int8.max, key: nextKey(), in: database)
        putGetValue(value: Int16.max, key: nextKey(), in: database)
        putGetValue(value: Int32.max, key: nextKey(), in: database)
        putGetValue(value: Int64.max, key: nextKey(), in: database)
        
        putGetValue(value: UInt.max, key: nextKey(), in: database)
        putGetValue(value: UInt8.max, key: nextKey(), in: database)
        putGetValue(value: UInt16.max, key: nextKey(), in: database)
        putGetValue(value: UInt32.max, key: nextKey(), in: database)
        putGetValue(value: UInt64.max, key: nextKey(), in: database)
        
        // Floats
        putGetValue(value: Float.leastNormalMagnitude, key: nextKey(), in: database)
        putGetValue(value: Double.leastNormalMagnitude, key: nextKey(), in: database)
        
    }

    func testPutGetWithWrappingTransaction() {

        var environment: Environment
        var database1: Database
        var database2: Database
        do {
            environment = try Environment(path: envPath, flags: [], maxDBs: 32)
            let db1Name = "\(#function)1"
            let db2Name = "\(#function)2"
            database1 = try environment.openDatabase(named: db1Name, flags: [.create])
            database2 = try environment.openDatabase(named: db2Name, flags: [.create])
        } catch {
            XCTFail(error.localizedDescription)
            fatalError()
        }
        
        let key = "key"
        let beforeValue = "before"
        let afterValue = "after"
        putGetValue(value: beforeValue, key: key, in: database1)
        putGetValue(value: beforeValue, key: key, in: database2)
        do {
            try environment.write { transaction in
                putGetValue(value: afterValue, key: key, in: database1, withTransaction: transaction)
                putGetValue(value: afterValue, key: key, in: database2, withTransaction: transaction)
                return .abort
            }
        } catch {
            XCTFail(error.localizedDescription)
            fatalError()
        }

        checkValue(value: beforeValue, key: key, in: database1)
        checkValue(value: beforeValue, key: key, in: database2)
        
        do {
            try environment.write { transaction in
                putGetValue(value: afterValue, key: key, in: database1, withTransaction: transaction)
                putGetValue(value: afterValue, key: key, in: database2, withTransaction: transaction)
                return .commit
            }
        } catch {
            XCTFail(error.localizedDescription)
            fatalError()
        }

        checkValue(value: afterValue, key: key, in: database1)
        checkValue(value: afterValue, key: key, in: database2)

    }

    
    
    func testCount() {
        let dbName = #function
        let database = createDatabase(named: dbName)
        XCTAssertEqual(database.count, 0)
        let count = 10
        
        do {
            
            for i in 0..<count {
                try database.put(value: "value-\(i)", forKey: "key-\(i)", withTransaction: nil)
            }
            
            XCTAssertEqual(count, database.count)
            
        } catch {
            XCTFail(error.localizedDescription)
        }
        
    }
    
    func testEmptyKey() {

        let database = createDatabase(named: #function)
        
        XCTAssertThrowsError(
            try database.put(value: "test", forKey: "", withTransaction: nil)
        )

    }
    
    func testDelete() {
        
        let database = createDatabase(named: #function)
        let key = "deleteTest"
        
        do {
            // Put a value
            try database.put(value: "Hello world!", forKey: key, withTransaction: nil)
            
            // Delete the value.
            try database.deleteValue(forKey: key)
            
            // Get the value
            let retrievedData = try database.get(type: Data.self, forKey: key, withTransaction: nil)
            XCTAssertNil(retrievedData, "Value still present after delete.")
        } catch {
            XCTFail(error.localizedDescription)
        }

    }
    
    func testDropDatabase() {
        
        let environment: Environment
        var database: Database!
        
        // Open a new database, creating it in the process.
        do {
            environment = try Environment(path: envPath, flags: [], maxDBs: 32)
            database = try environment.openDatabase(named: "dropTest", flags: [.create])
        } catch {
            XCTFail(error.localizedDescription)
            return
        }
        
        // Close the database and drop it.
        do {
            
            // Drop the database and get rid of the reference, so that the handle is closed.
            try database.drop()
            database = nil

        } catch {
            XCTFail(error.localizedDescription)
            return
        }
        
        // Attempt to open a database with the same name. We aren't passing in the .create flag, so this action should fail, indicating that the database was dropped successfully.
        do {
            database = try environment.openDatabase(named: #function)
        } catch {

            // The desired outcome is that the database is not found.
            if let lmdbError = error as? LMDBError {
                
                switch lmdbError {
                case .notFound: return
                default: break
                }
                
            }
            
            XCTFail(error.localizedDescription)
            return
            
        }
        
        XCTFail("The database was not dropped.")
        return
        
    }
    
    func testEmptyDatabase() {
        
        let database = createDatabase(named: #function)
        
        let key = "test"
        do {
            // Put a value
            try database.put(value: "Hello world!", forKey: key, withTransaction: nil)

            // Empty the database.
            try database.empty()
            
            // Get the value. We want the result to be nil, because the database was emptied.
            let retrievedData = try database.get(type: Data.self, forKey: key, withTransaction: nil)
            XCTAssertNil(retrievedData, "Value still present after database being emptied.")
            
        } catch {
            XCTFail(error.localizedDescription)
            return
        }
        
    }
    
    static var allTests : [(String, (SwiftLMDBTests) -> () throws -> Void)] {
        return [
            ("testGetLMDBVersion", testGetLMDBVersion),
            ("testCreateEnvironment", testCreateEnvironment),
            ("testCreateUnnamedDatabase", testCreateUnnamedDatabase),
            ("testHasKey", testHasKey),
            ("testPutGet", testPutGet),
            ("testEmptyKey", testEmptyKey),
            ("testDelete", testDelete),
            ("testDropDatabase", testDropDatabase),
            ("testEmptyDatabase", testEmptyDatabase),
        ]
    }

    
}
