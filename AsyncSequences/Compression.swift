//
//  Compression.swift
//  Compression
//
//  Created by Chris Eidhof on 20.08.21.
//

import Foundation
import Compression

struct CompressorError: Error {
}

class Compressor {
    enum Method {
        case compress
        case decompress
    }
    static let bufferSize = 32_768

    let streamPointer: UnsafeMutablePointer<compression_stream>
    let destinationBufferPointer: UnsafeMutablePointer<UInt8>
    
    var stream: compression_stream {
        get { streamPointer.pointee }
        set { streamPointer.pointee = newValue }
    }
    
    deinit {
        compression_stream_destroy(streamPointer)
        streamPointer.deallocate()
    }
    
    init(method: Method) {
        streamPointer = UnsafeMutablePointer.allocate(capacity: 1)
        compression_stream_init(streamPointer, method == .compress ? COMPRESSION_STREAM_ENCODE : COMPRESSION_STREAM_DECODE, COMPRESSION_ZLIB)
        
        // todo see if we can use Data
        destinationBufferPointer = UnsafeMutablePointer<UInt8>.allocate(capacity: Self.bufferSize)
        stream.src_size = 0
        stream.dst_size = Self.bufferSize
        stream.dst_ptr = destinationBufferPointer
    }
    
    var remainingData = Data()
    
    private func processRemainingData(finalize: Bool) throws -> Data {
        var result = Data()
        
        var status: compression_status
        repeat {
            stream.src_size = remainingData.count
            status = remainingData.withUnsafeBytes { bp in
                let bufferPointer = bp.bindMemory(to: UInt8.self)
                stream.src_ptr = bufferPointer.baseAddress!
                return compression_stream_process(&stream, finalize ? Int32(COMPRESSION_STREAM_FINALIZE.rawValue) : 0)
            }
            remainingData = stream.src_size == 0 ? Data() : Data(remainingData.suffix(stream.src_size))
            switch status {
            case COMPRESSION_STATUS_END, COMPRESSION_STATUS_OK:
                let bytesAvailable = Self.bufferSize - stream.dst_size
                stream.dst_ptr = destinationBufferPointer
                stream.dst_size = Self.bufferSize
                if bytesAvailable == 0 && status != COMPRESSION_STATUS_END {
                    return result
                }
                let bytes = Data(bytesNoCopy: destinationBufferPointer, count: bytesAvailable, deallocator: .none)
                result.append(bytes)
            case COMPRESSION_STATUS_ERROR:
                throw CompressorError()
            default:
                fatalError()
            }
        } while status == COMPRESSION_STATUS_OK
        return result
    }
    
    func compress(_ data: Data) throws -> Data {
        assert(data.count <= Self.bufferSize)
        if remainingData.isEmpty {
            remainingData = data
        } else {
            remainingData.append(data)
        }
        return try processRemainingData(finalize: false)
    }
    
    func eof() throws -> Data {
        return try processRemainingData(finalize: true)
    }
}
