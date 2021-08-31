//
//  ContentView.swift
//  AsyncSequences
//
//  Created by Chris Eidhof on 23.08.21.
//

import SwiftUI

struct ContentView: View {
    var body: some View {
        Text("Hello, world!")
            .padding()
            .task {
                try! await sample()
            }
    }
}

