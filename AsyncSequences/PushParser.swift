import Foundation
import libxml2

enum XMLEvent: Equatable {
    case didStart(elementName: String)
    case didEnd(elementName: String)
    case foundCharacters(String)
}

class PushParser {
    var context: xmlParserCtxtPtr? = nil
    var handler = xmlSAXHandler()
    var onEvent: (XMLEvent) -> () = { _ in fatalError() }
    
    deinit {
        xmlFreeParserCtxt(context)
    }
    
    func process(_ data: Data) {
        data.withUnsafeBytes { bytes in
            let b = bytes.bindMemory(to: CChar.self)
            let result = xmlParseChunk(context, b.baseAddress, Int32(data.count), 0)
            assert(result == 0)
            return
        }
    }
    
    func finish() {
        xmlParseChunk(context, nil, 0, 1)

    }
    
    init(filename: String = "") {
        func onStart(context: UnsafeMutableRawPointer?,
                     fullName: UnsafePointer<UInt8>?, attrs: UnsafeMutablePointer<UnsafePointer<UInt8>?>?) {
            let zelf: PushParser = Unmanaged.fromOpaque(context!).takeUnretainedValue()
            let str = String(cString: fullName!)
            zelf.onEvent(.didStart(elementName: str)) // todo attributes
        }
        
        func onEnd(context: UnsafeMutableRawPointer?,
                     fullName: UnsafePointer<UInt8>?) {
            let zelf: PushParser = Unmanaged.fromOpaque(context!).takeUnretainedValue()
            let str = String(cString: fullName!)
            zelf.onEvent(.didEnd(elementName: str)) // todo attributes
        }
        
        func onChars(context: UnsafeMutableRawPointer?,
                     characters: UnsafePointer<UInt8>?, count: Int32) {
            let zelf: PushParser = Unmanaged.fromOpaque(context!).takeUnretainedValue()
            let str = String(String(cString: characters!).prefix(Int(count)))
            zelf.onEvent(.foundCharacters(str))
        }
        
        handler.initialized = XML_SAX2_MAGIC
        handler.startElement = onStart
        handler.endElement = onEnd
        handler.characters = onChars
        let selfRef = Unmanaged.passUnretained(self)
        context = xmlCreatePushParserCtxt(&handler, selfRef.toOpaque(), nil, 0, filename)
    }
}
