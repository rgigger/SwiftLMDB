//
//  Transaction.swift
//  SwiftLMDB
//
//  Created by Rick Gigger on 01/08/2020.
//  Copyright © 2016 Rick Gigger. All rights reserved.
//

import Foundation
import LMDB

/// All read and write operations on the database happen inside a Transaction.
public struct Cursor {
    internal private(set) var handle: OpaquePointer?
    let transaction: Transaction
    
    internal init(database: Database, transaction: Transaction) throws {
        self.transaction = transaction
        
        // http://www.lmdb.tech/doc/group__mdb.html#ga9ff5d7bd42557fd5ee235dc1d62613aa
        let cursorStatus = mdb_cursor_open(transaction.handle, database.handle, &handle)
        
        guard cursorStatus == MDB_SUCCESS else {
            throw LMDBError(returnCode: cursorStatus)
        }
    }
    
    public func first() throws -> (key: Data, value: Data) {
        var keyVal = MDB_val()
        var dataVal = MDB_val()
        var status: Int32 = 0
        status = mdb_cursor_get(handle, &keyVal, &dataVal, MDB_FIRST)
        guard status != MDB_NOTFOUND else {
            // TODO: fix this error so it makes sense
            throw LMDBError(returnCode: -1)
        }
        guard status == MDB_SUCCESS else {
            // TODO: fix this error so it makes sense
            throw LMDBError(returnCode: -1)
        }

        let keyData = Data(bytes: keyVal.mv_data, count: keyVal.mv_size)
        let valData = Data(bytes: dataVal.mv_data, count: dataVal.mv_size)
        
        return (keyData, valData)
    }
}
