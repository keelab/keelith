// Package di builds an instance-scoped application dependency graph.
//
// Constructors remain ordinary Go functions. Resolution happens only while a
// graph is built; containers are never injected into application code or used
// as request-path service locators.
package di
