// swift-tools-version:5.1
//  Package.swift
//  Perfect-PostgreSQL
//
//  Created by Kyle Jessup on 2019/12/30.
//	Copyright (C) 2019 PerfectlySoft, Inc.
//
//===----------------------------------------------------------------------===//
//
// This source file is part of the Perfect.org open source project
//
// Copyright (c) PerfectlySoft Inc. and the Perfect project authors
// Licensed under Apache License v2.0
//
// See http://perfect.org/licensing.html for license information
//
//===----------------------------------------------------------------------===//
//
import PackageDescription

let package = Package(
	name: "PerfectODBC",
	products: [
		.library(name: "PerfectODBC", targets: ["PerfectODBC"])
	],
	dependencies: [
		.package(url: "https://github.com/PerfectlySoft/Perfect-CRUD.git", .branch("master"))
	],
	targets: [
		.target(name: "PerfectODBC", dependencies: ["PerfectCRUD", "unixodbc"]),
		.systemLibrary(name: "unixodbc",
					   pkgConfig: "odbc",
					   providers: [.brew(["unixodbc"])]),
		.testTarget(name: "PerfectODBCTests", dependencies: ["PerfectODBC"])
	]
)
