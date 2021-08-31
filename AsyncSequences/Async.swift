//
//  Async.swift
//  Async
//
//  Created by Chris Eidhof on 23.08.21.
//

import Foundation

let onlineURL = URL(string: "https://d2sazdeahkz1yk.cloudfront.net/sample/enwik8.zlib")!

struct Page {
    var title: String
     var id: String
}

func stream(at url: URL) -> AsyncStream<Data>{
    let file = fopen(url.path, "r")
    let buffer = UnsafeMutableRawPointer.allocate(byteCount: Compressor.bufferSize, alignment: 1)
    
    func close() {
        fclose(file)
        buffer.deallocate()
    }

    return AsyncStream {
        let count = fread(buffer, 1, Compressor.bufferSize, file)
        guard count > 0 else {
            close()
            return nil
        }
        return Data(bytes: buffer, count: count)
    }
}

func sample() async throws {
    let start = Date.now
//    let url = Bundle.main.url(forResource: "enwik8", withExtension: "zlib")!
    let url = URL(string: "/Users/chris/Downloads/enwik9.xml")!
//    let fileHandle = try FileHandle(forReadingFrom: url)
    let bytes = stream(at: url)
    for try await page in bytes.xmlEvents.pages {
        print(page)
    }
    print("Duration: \(Date.now.timeIntervalSince(start))")
}

extension AsyncSequence where Element == XMLEvent {
    var pages: AsyncThrowingStream<Page, Error> {
        var it = makeAsyncIterator()
        return AsyncThrowingStream(unfolding: {
            try await it.parsePage()
        })
    }
}

extension AsyncSequence where Element == UInt8 {
    var chunked: Chunked<Self> {
        Chunked(base: self)
    }
}

struct Chunked<Base: AsyncSequence>: AsyncSequence where Base.Element == UInt8 {
    var base: Base
    var chunkSize: Int = Compressor.bufferSize // todo
    typealias Element = Data
    
    struct AsyncIterator: AsyncIteratorProtocol {
        var base: Base.AsyncIterator
        var chunkSize: Int
        
        mutating func next() async throws -> Data? {
            var result = Data()
            while let element = try await base.next() {
                result.append(element)
                if result.count == chunkSize { return result }
            }
            return result.isEmpty ? nil : result
        }
    }
    
    func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(base: base.makeAsyncIterator(), chunkSize: chunkSize)
    }
}

extension AsyncSequence where Element == Data {
    var decompressed: Compressed<Self> {
        Compressed(base: self, method: .decompress)
    }
}

struct Compressed<Base: AsyncSequence>: AsyncSequence where Base.Element == Data {
    var base: Base
    var method: Compressor.Method
    typealias Element = Data
    
    struct AsyncIterator: AsyncIteratorProtocol {
        var base: Base.AsyncIterator
        var compressor: Compressor
        
        mutating func next() async throws -> Data? {
            if let chunk = try await base.next() {
                return try compressor.compress(chunk)
            } else {
                let result = try compressor.eof()
                return result.isEmpty ? nil : result
            }
        }
    }
    
    func makeAsyncIterator() -> AsyncIterator {
        let c = Compressor(method: method)
        return AsyncIterator(base: base.makeAsyncIterator(), compressor: c)
    }
}

extension AsyncSequence where Element: Sequence {
    var flattened: Flattened<Self> {
        Flattened(base: self)
    }
}

struct Flattened<Base: AsyncSequence>: AsyncSequence where Base.Element: Sequence {
    var base: Base
    typealias Element = Base.Element.Element
    
    struct AsyncIterator: AsyncIteratorProtocol {
        var base: Base.AsyncIterator
        var buffer: Base.Element.Iterator?
        
        mutating func next() async throws -> Element? {
            if let el = buffer?.next() {
                return el
            }
            buffer = try await base.next()?.makeIterator()
            guard buffer != nil else { return nil }
            return try await next()
        }
    }
    
    func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(base: base.makeAsyncIterator())
    }
}

extension AsyncSequence where Element == Data {
    var xmlEvents: XMLEvents<Self> {
        XMLEvents(base: self)
    }
}

struct XMLEvents<Base: AsyncSequence>: AsyncSequence where Base.Element == Data {
    var base: Base
    typealias Element = XMLEvent
    
    struct AsyncIterator: AsyncIteratorProtocol {
        var base: Base.AsyncIterator
        let parser = PushParser()
        var buffer: [XMLEvent] = []
        
        mutating func next() async throws -> Element? {
            if !buffer.isEmpty {
                return buffer.removeFirst()
            }
            if let data = try await base.next() {
                var newEvents: [XMLEvent] = []
                parser.onEvent = { event in
                    newEvents.append(event)
                }
                parser.process(data)
                buffer = newEvents
                return try await next()
            }
            parser.finish()
            return nil
        }
    }
    
    func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(base: base.makeAsyncIterator())
    }
}

extension AsyncIteratorProtocol where Element == XMLEvent {
    mutating func parsePage() async throws -> Page? {
        while let event = try await next() {
            switch event {
            case .foundCharacters(_): continue
            case .didEnd(elementName: _): continue
            case .didStart(elementName: "page"):
                return try await parsePageContents()
            case .didStart(elementName: _):
                continue // todo
            }
        }
        return nil
    }
    
    mutating func parsePageContents() async throws -> Page {
        var title: String?
        var id: String?
        while let event = try await next() {
            switch event {
            case .didStart(elementName: "title"):
                title = try await parseCharacters(until: "title")
            case .didStart(elementName: "id"):
                id = try await parseCharacters(until: "id")
            case let .didStart(elementName: name):
                try await parseChildren(until: name)
            case .didEnd(elementName: "page"):
                guard let t = title, let i = id else {
                    throw ParseError()
                }
                return Page(title: t, id: i)
            case .foundCharacters:
                continue
            default:
                print(event)
                throw ParseError()
            }
        }
        throw ParseError()
    }
    
    mutating func parseChildren(until tag: String) async throws {
        while let event = try await next() {
            switch event {
            case .foundCharacters: continue
            case let .didStart(elementName: name):
                try await parseChildren(until: name)
            case .didEnd(elementName: tag):
                return
            default:
                throw ParseError()
            }
        }
        throw ParseError()
    }
    
    mutating func parseCharacters(until tag: String) async throws -> String {
        var result = ""
        while let event = try await next() {
            switch event {
            case .foundCharacters(let str):
                result += str
            case .didEnd(elementName: tag):
                return result
            default:
                throw ParseError()
            }
        }
        throw ParseError()
    }
}

struct ParseError: Error {}
